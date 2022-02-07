<?php

class Engine {
	public $type = ""; // Oracle, SQL Server, etc.
	public $version = ""; // 12.3 for Oracle 12c, etc.
	public $datatypes = array(
		"int"=>"int",
		"float"=>"float",
		"text"=>"varchar",
		"intl_text"=>"varchar"
	);
	public $dateFormat = 'Y-m-d';
    public $admin_email = "";
    public $errors = array();
	public $placeholderRegexp = "/\\?/";
	public $sql_reserved_words = array("all", "analyse", "analyse", "and", "any", "as", "asc", "at", "authorization", "between", "binary", "both", "case", "cast", "check", "collate", "column", "constraint", "cross", "current_date", "current_role", "current_time", "current_timestamp", "current_user", "desc", "distinct", "do", "else", "end", "except", "false", "for", "from", "full", "group", "having", "in", "initially", "inner", "intersect", "join", "left", "limit", "localtime", "localtimestamp", "new", "not", "notnull", "null", "off", "offset", "on", "only", "outer", "or", "order", "primary", "references", "right", "select", "session_user", "some", "symmetric", "table", "then", "to", "true", "user", "using", "verbose", "when", "where");

	
	// Alias these as needed in the individual engines!
	
	// Connect to the DB. 
	public function connect($params = array()){
		return true;
	}
    
    // Shared connection cleanup; call this in connect()
    public function connect_shared($connection_params){
        $this->admin_email = $connection_params["admin_email"];
    }
	
	// Get a select query result. Return a result array.
	public function get($query, $column_or_row = 'column'){
		return array();
	}
	
	// Execute a non-select query (e.g. insert, delete). Return true or false.
	public function execute($query, $params=array()){
		return false;
	}
	
	// If not otherwise defined, query is an alias of parse
	public function query($query, $params=array()){
		return $this->parse($query, $params);
	}
	
	// If not otherwise defined, parse is an alias of prepare
	public function parse($query, $params=array()){
		return prepare($query, $params);
	}
	
	// Prepare a query with bind variables. Alias this if there is a real way to do it
	public function prepare($query, $params=array()){
		return str_replace("?", $params, $query);
	}
	
	// Pull an array of all the placeholders from a query.
	// UNTESTED
	public function getPlaceholdersFromQuery($query){
		preg_match_all($this->placeholderRegexp, $query, $matches);
		return $matches;
	}
	
	// Create a bind variable placeholder for use in the string. By default, placeholders are '?'
	public function createPlaceholder($count){
		return '?';
	}
	
	// Create a comma-separated list of N placeholders
	public function placeholderList($N, $startN = 0){
		$placeholders = array();
		
		for($i = ($startN + 1); $i <= ($N + $startN); $i++){
			$placeholders[$i] = $this->createPlaceholder($i);
		}
		
		return implode(",", $placeholders);
	}
	
	// Return an array of table names in the database
	public function tables($schema=""){
		return array();
	}
	
	// Given a table, return an array of column names
	public function columns($table, $schema=""){
		return array();
	}
	
	// Given a column name and table (and schema), return the data type
	public function column_datatype($column, $table, $schema=""){
		return "";
	}
	
	// Given a table, return an array of column name=>datatype
	public function column_datatypes($table, $schema=""){
		return array();
	}
	
	// Given a field name, return it in AUTO INCREMENT PRIMARY KEY syntax
	public function auto_increment_primary_key_field($fieldname){
		return "";
	}
	
	// Executes some auto increment primary key 'cleanup' (queries to perform after creating the table)
	public function auto_increment_primary_key_cleanup(){
		return true;
	}
	
	// Given a field name, return it in LISTAGG syntax
	public function listagg_field($fieldname, $table=""){
		return $fieldname;
	}
    
    // Given an array of WHERE subclauses, return the full WHERE clause
    public function wherefields_to_string($wherefields=array()){
        if(empty($wherefields)){
            return "";
        }
        else{
            return " WHERE " . implode(" AND ", $wherefields);
        }
    }

    // CREATE TABLE query with optional parameters, including unique constraints (relies on primary_key_clause function)
    // This works for Oracle and, if the primary_key_clause function is up to date for others, should work for them too
    public function create_query($columns, $table, $unique_contraints = array(), $primaryKeyField = ""){
		$colstring = "";
		$primaryKeyAdded = false;
			
			// Add the column names & types to the query string
			foreach($columns as $name=>$type){
				if($colstring != "") $colstring .= ",";
								
				// If this is the primary key field, add the constraints
				if($primaryKeyField != "" && strtolower($name) == strtolower($primaryKeyField)){
					$colstring .= $this->primary_key_clause($name);
					$primaryKeyAdded = true;
				}
				else{
					$colstring .= $name." ".$type;
				}
			}
			
			// Add primary key field, if one is set and it wasn't in the column list
			if($primaryKeyField != "" && !$primaryKeyAdded){
				$colstring = $this->auto_increment_primary_key_field($primaryKeyField).", ".$colstring;
			}

            // Add unique constraints, if any                             
            if(!empty($unique_constraints)){
                foreach($unique_constraints as $constraint_name=>$fields){
                    $colstring .= ", CONSTRAINT " . $constraint_name . " UNIQUE (" . implode(",", $fields) . ")";
                }
            }
                        
			
			// Create the query
			$query = "CREATE TABLE " . $table . "(" . $colstring . ")";

			return $query;
    }

    // Given a set of columns and a table, check if there are new columns in the set that aren't in the table 
    // If not, return false
    // If so, return the query to add the columns 
    // Optimized for Oracle but move/alias if needed
	public function add_columns_query($columns, $table, $schema=""){
		$query = "";

		// Get the columns that currently exist in the table & compare with proposed columns
		$currentTableColumns = array_keys(array_change_key_case($this->column_datatypes($table, $schema)));
		$proposedColumns = array_keys($columns);
		$extraColumns = array_diff($proposedColumns, $currentTableColumns);

		if(empty($extraColumns)){ // No new columns
			return false;
		}
		else{ // New columns!

			$colstring = "";
			foreach($extraColumns as $key){
				if($colstring != "") $colstring .= ", ";
				$colstring .= $key;
				$colstring .= " " . $columns[$key];
			}

			$query = "ALTER TABLE ";
			if($schema != "") $query .= $schema;
			$query .= $table;
			$query .= " ADD (";
			$query .= $colstring;
			$query .= ")";
		}

		return $query;
	}
	
	// Given a table and an array in the form (columnname=>value, columnname=>value)
	// return an INSERT query
	public function insert_query($values, $table){
		$query = "";
		
		if(isset($values[0]) && is_array($values[0])){
			$query = $this->multiple_insert_query($values, $table);
		}
		
		if($query == ""){
			$insclause = $this->insert_clause($values, $table);
			$query = "INSERT INTO ".$table." ".$insclause;
		}
		
		return $query;
	}
	
	// Support for INSERT
	// Given a table and an array of key=>value, return string in form: 
	// (col1, col2) VALUES (val1, val2)
	public function insert_clause($values, $table="", $schema="", $placeholderNext = 1){
		$colstring = "";
		$valuestring = "";
		
		$column_datatypes = array();
		if($table != ""){
			$column_datatypes = $this->column_datatypes($table, $schema);
		}
		
		foreach($values as $key=>$value){
			$datatype = "";
			if(isset($column_datatypes[$key])){
				$datatype = $column_datatypes[$key];
			}
			if($datatype == ""){
				if(is_valid_date($value)){
					$datatype = "date";
				}
				else if($value === TRUE || $value === FALSE){
					$datatype = "bool";
				}
				else if(is_integer($value)){
					$datatype = "int";
				}
				else if(is_numeric($value)){
					$datatype = "number";
				}
			}
			
			if($colstring != ""){
				$colstring .= ",";
			}

			$colstring .= $key;

			if($valuestring != ""){
				$valuestring .= ",";
			}

			/*
			if($this->is_already_dbized($value)){
				$valuestring .= $value;
			}
			else{
				$valuestring .= $this->dbize($value, $datatype);
			}
			*/
			$valuestring .= $this->createPlaceholder($placeholderNext);
			$placeholderNext++;
		}
		
		$clause = "(".$colstring.") VALUES(".$valuestring.")";
		return $clause;
	}
	
	// Support for insert: insert multiple rows 
	// The default format is: 
	// INSERT INTO MyTable ( Column1, Column2 ) VALUES ( Value1, Value2 ), ( Value1, Value2 )
	// For another format, alias (e.g. it is aliased in EngineOracle)
	public function multiple_insert_query($values, $table, $placeholderNext = 1){
		list($colstring, $clauses) = multiple_insert_clauses($values, $placeholderNext);
		$query = "INSERT INTO ".$table." ".$colstring." ".implode(",",$clauses);
		return $query;
	}
	
	// Support function for MULTIPLE INSERT 
	// Given a table and an array of arrays of key=>value, return array of strings in form:
	// ["(col1,col2) VALUES(val1,val2)","(col1,col2) VALUES(vala,valb)"]
	public function multiple_insert_clauses($values, $placeholderNext = 1){
		$clauses = array();
		$colstring = "";
		
		foreach($values as $idx=>$valarr){
			
			if($idx==0){
				$colstring = "(".implode(",",array_keys($valarr)).")";
			}
			
			$numVals = count($valarr);
			$clauses[] = "(". $this->placeholderList($numVals, $placeholderNext) . ")";
			$placeholderNext += $numVals;
		}
		
		return array($colstring, $clauses);
	}
    
    // Create the UPDATE query
    public function update_query($table, $values, $selectors, $wherefields = array(), $schema = "", $placeholderNext = 1){
        
        $query = "";
        $setstring = "";
            
        // Perform the UPDATE
        foreach($values as $key=>$value){

            /* Skip selectors
            if(isset($selectors[$key])){
                continue;
            }*/

            // Build 'SET' string
            if($setstring != "") $setstring .= ",";
			$datatype = $this->column_datatype($key, $table, $schema);
			
			$setstring .= $key . "=" . $this->createPlaceholder($placeholderNext);
			$placeholderNext++;
        }
        
        if($setstring != "") $setstring = " SET " . $setstring;

        // Add selectors to WHERE clause
        foreach($selectors as $key=>$value){
			$datatype = $this->column_datatype($key, $table, $schema);
            $wherefields[] = "$key = " . $this->createPlaceholder($placeholderNext);
			$placeholderNext++;
        }

        $wherestring = $this->wherefields_to_string($wherefields);

        if($setstring != "" && $wherestring != ""){
            $query = "UPDATE ";
			if($schema != "") $query .= $schema . ".";
			$query .= $table . $setstring . $wherestring;
            $query = $query;
        }
        
        return $query;
        
    }
    
    // Create the MULTI UPDATE query
    // This is a fallback which just creates multiple UPDATE queries. 
    // Individual engines may have better ways to deal with this in a single query.
    // This returns an ARRAY of queries; 
    public function multi_update_query($table, $values_multiarr, $selectors_multiarr, $wherefields = array(), $schema = "", $placeholderNext = 1){

        $queries = array();
        
        foreach($values_multiarr as $idx=>$values){
            $queries[$idx] = $this->update_query($table, $values, $selectors_multiarr[$idx], $wherefields, $schema, $placeholderNext);
			$placeholderNext += count($values);
			$placeholderNext += count($selectors_multiarr[$idx]);
        }
        
        return $queries;        
    }
	
	// Just returns the connection type
	public function connection_type(){
		return $this->type;
	}
	
	// Typecast syntax
	// Alias in individual engines - this just returns the column name
	public function typecast($col, $type){
		return $col;
	}
	
	// Encode values for saving in database
	public function dbize($value, $datatype = "", $addQuotes = true, $unencodeQuotes = false){	
		if(is_string($datatype)) $datatype = strtolower($datatype);
		
		// Any data type can be null. Don't want this to be overwritten with empty string, etc.
		if($value === null) return null;
		if($value === "") return null;
		
		if(($datatype == "boolean" || $datatype == "bool") || ($datatype == "" && ($value === TRUE || $value === FALSE))){
			$ret = $this->dbize_bool($value);
		}            
		else if($datatype == "date"){
			$ret = $this->dbize_date($value);
		}
		else if(is_integer($value) || $datatype == "int"){
			$ret = $this->dbize_int($value);
		}
		else if($datatype == "number" || $datatype == "float" || $datatype == "double precision" || is_float($value) || is_int($value)){
			$ret = $this->dbize_num($value);
		}		
		else if(is_array($value)){
			$ret = $this->dbize_array($value);
		}
		else{
			$encoded = $this->dbize_text($value, $addQuotes, $unencodeQuotes);
			$ret = $encoded;
		}

		return $ret;
	}
	
	// Support functions for dbize. Some may be aliased
	public function is_already_dbized($value){ // Return true if the value doesn't need to be 'db-ized' because it already is (a number or date function or whatever)
		return false;
	}
	
	public function dbize_bool($value){
				
		if($value){
			$ret = 'TRUE';
		}
		else{
			$ret = 'FALSE';
		}
		
		return $ret;
	}
	
	public function dbize_int($value){
		return intval($value);
	}
	
	public function dbize_num($value){
		return floatval($value);
	}
	
	public function dbize_date($value){
		
		if(!is_integer($value)){
			$timestamp = strtotime($value);
		}
		else{
			$timestamp = intval($value);
		}
		
		$date = date('d/m/Y', $timestamp);

		return $date;
	}
	
	public function dbize_array($value){
            $ret = array();
            foreach($value as $k=>$v){
                $ret[$k] = $this->dbize($v, false);
            }
            return $ret;
	}
	
	public function dbize_text($value, $addQuotes = true, $unencodeQuotes = false){
		$ret = "";
		
		if($addQuotes){
			$ret .= "'";
		}
		
		// Unencode before encoding to prevent double-encoding
		$value = trim($value);
		//$value = utf8_encode($value); // Would be needed if the source isn't utf-8 encoded, or else using utf-8 htmelentities will return an empty string. But it can also garble things that are already utf-8 encoded. Sigh.
		
		$unencoded = html_entity_decode($value, ENT_QUOTES, 'UTF-8');
		
		// Return unencoded but slashed quotes rather than HTML encoded.
		// This is best for matching in filters, NOT for saving values. 
		if($unencodeQuotes){
			$encoded = str_replace("'", "''", $unencoded);
			
		}
		else {
			$encoded = htmlentities($unencoded, ENT_QUOTES, 'UTF-8', false);
		}

		// Use the encoded value, unless htmlentities ate it
		if(!$encoded){
			$encoded = $value;
		}
                
      	// Set return value
		$rettext = $encoded;
		$ret .= $rettext;

		if($addQuotes){
			$ret .= "'";
		}
                
		return $ret;
	}
	
	// Unencode values pulled FROM the database
	public function undbize($resval){
            
		if($resval instanceof DateTime){
                    return $resval->format('Y-m-d H:i:s');
		}
		else{
                    $decoded = html_entity_decode($resval, ENT_QUOTES, 'UTF-8');
                    return $decoded;
                    
		}
	}
        
        // for use with array_walk
        function dbize_filter(&$a){
            $b = htmlspecialchars($a, ENT_QUOTES, 'UTF-8');
            return $b;
        }
	
	// Take a value and return an array of (datatype, size)
	function value_to_datatype($value){
		$datatype = "";
		$datasize = binary_magnitude(strlen($this->dbize($value))*10);
		
		if(strtotime($value)){
                    $datatype = $this->datatypes["datetime"];
                }
		else if(is_bool($value)){
                    $datatype = $this->datatypes["bool"];
                }
		else if(is_int($value)){
                    $datatype = $this->datatypes["int"];
                }
		else if(is_numeric($value)){
                    $datatype = $this->datatypes["float"];
                }
		else if(is_utf8($value)){
                    $datatype = $this->datatypes["intl_text"];
                }
		else{
                    $datatype = $this->datatypes["text"];
                }

		return array($datatype, $datasize);
	}
	
	// Return a string with date math
	// Include both an interval string ('1 month') and a number of days (30) since different engines require different
	// The default simply uses the number of days
	// (timestamp, +, '1 month', 28) yields "(timestamp + 28)"
	function datemath($date, $operator = "+", $intervalstring = "1 day", $numdays = 1){
		return "(" . $date . " " . $operator . " " . $numdays . ")";
	}
	
	// Take an array of rows (each row being columname => value) and return a single level array of columnname => datatype
	function rows_to_datatypes($rows){
		$datatypes = array();
		$datasizes = array();
		$hierarchy = array("bool","int","float","nvarchar2","nvarchar","varchar2","varchar");
		$hierarchy_keys = array_flip($hierarchy);
		
		foreach($rows as $row){
			foreach($row as $colname=>$value){
				
				list($datatype, $size) = value_to_datatype($value);
				
				// If the datatype is already set, pick the LARGER of the two in the hierarchy (higher rank = more freedom for value types)
				if(isset($datatypes[$colname])){
					$current_datatype = $datatypes[$colname];
					$datatype_rank = max($hierarchy_keys[$current_datatype],$hierarchy_keys[$datatype]);
					$datatype = $hierarchy[$datatype_rank];
				}
				$datatypes[$colname] = $datatype;

				// If the datasize is already set, pick the LARGER of the two (more size = more freedom)
				if(isset($datasizes[$colname])){
					$size = max($datasizes[$colname], $size);
				}
				$datasizes[$colname] = $size;
			}
		}
		
		// To finish up, add the size to the varchars
		foreach($datatypes as $colname=>$datatype){
			if(strpos($datatype,'varchar')){
				$datasize = $datasizes[$colname];
				$datatypes[$colname] .= "(".$datasize.")";
			}
		}
		
		return $datatypes;
	}
	
	// Take an array of columnname => value and turn it into an array of columnname => datatype
	function row_to_datatypes($row){
		$datatypes = array();
		
		foreach($row as $colname=>$value){
			list($datatype, $datasize) = $this->value_to_datatype($value);
			
                        if(strpos($datatype,'varchar')){
                            $datatype .= "(".$datasize.")";
                        }
			
                        $datatypes[$colname] = $datatype;
		}
		
		return $datatypes;
	}
	
	// Hash password
	public function hash_password($plaintextpwd){
		$bcrypt = new Bcrypt(15);
		$hash = $bcrypt->hash($plaintextpwd);
		$isGood = $bcrypt->verify($plaintextpwd, $hash);
		if($isGood){
			return $hash;
		}
		else{
			return false;
		}
	}
	
	// Print rows as table
	public function rows_as_table($dbrows, $formats=array()){
		$ret = "";
		
		if(!empty($dbrows)){
			$ret .= "<table class='table table-bordered' border=1>";
			
			// Print the header
			$ret .= "<thead>";
			
			if(!empty($dbrows[0])){
				foreach($dbrows[0] as $key=>$val){
					$ret .= "<th>";
					$ret .= ucwords(str_replace("_", " ", strtolower($key)));
					$ret .="</th>";
				}
			}
			
			$ret .= "</thead>";
			
			// Print the body of the table
			$ret .= "<tbody>";
			
			if(!empty($dbrows) && is_array($dbrows)){
				foreach($dbrows as $dbrow){
					$ret .= "<tr>";
						if(!empty($dbrow) && is_array($dbrow)){
							foreach($dbrow as $id=>$val){
								
								if(is_array($formats) && isset($formats[$id])){
                                                                    $format = $formats[$id];
                                                                }
								else if(!is_array($formats)){
                                                                    $format = $formats; 
                                                                }
								
								$ret .= "<td>";
								
								$val = $this->undbize($val);
								
								if(isset($format)){
									$val = formatValue($val,$format);
								}

								$ret .= $val;
								$ret .="</td>";
							}
						}
					$ret .= "</tr>";
				}
				
				$ret .= "</tbody>";
				$ret .= "</table>";
			}
		}
			
		return $ret;
	}
	
	// File open/close version of CSV
	public function write_read_csv_file($query, $params=array(), $filename="export.csv", $foldername="downloads", $headings=true){
		
		$csvSaved = false;
		$filenameparts = explode(".", $filename);
		$tmpfilename = $filenameparts[0].rand(1000,10000); // Randomize temporary file name -- allows grabbing of specific file even if several are in process
		$tmpfilename .= "." . $filenamesparts[1]; // add extension back on
		$tmpfilename = $foldername."/".$tmpfilename;
		
		if (!file_exists($foldername) && !is_dir($foldername) && is_writable(getcwd())){
            mkdir($foldername, 0777, true);
		}
		if(!file_exists($foldername) && !is_dir($foldername)){
			throw new Exception("Could not create $foldername folder");
		}
		else{
		
			// Test open the file
			$fp = fopen($tmpfilename, 'w');
			fclose($fp);
			
			if(!isset($fp) || !$fp || !file_exists($tmpfilename)){
				throw new Exception("File $filename does not exist");
			}
			else if(!is_writable($tmpfilename)){
				throw new Exception("File $filename is not writable");
			}
			else{
			
				// Create file. Except method varies by Engine. 
				$csvSaved = $this->save_query_result_to_csv($query, $params, $tmpfilename, $headings);
				
				unlink($tmpfilename);
				unset($fp);
				flush();
			
				if(!$csvSaved){
					throw new Exception("CSV not saved");
				}
			}
			
		}

		// Reopen and print
		if($csvSaved && file_exists($tmpfilename) && is_writable($tmpfilename)){
			$fp = fopen($tmpfilename, 'r');
			rewind($fp);
			header('Content-Type: application/csv');
			header('Content-Disposition: attachment; filename="'.$filename."'");
			fpassthru($fp);
			fclose($fp);
			
			unlink($tmpfilename);
			unset($fp);
			flush();
			
			return true;
		}
		
		return $tmpfilename;
	}
	
	// Perform the CSV export
	// Return filename or false
	public function get_csv($query, $params=array(), $filename="export.csv", $foldername="downloads", $headings=true){	
	
		//$filename .= ".csv";
		
		try{
			$newfilename = $this->write_read_csv_file($query, $params, $filename, $foldername, $headings);
			return $newfilename;
		}
		catch(Exception $e){
			// Could not open file - file was not created successfully
			header('Content-Type: application/csv');
			header('Content-Disposition: attachment; filename="'.$filename.'"');
			$this->print_plaintext_as_csv($query, $params, $headings);
			return false;
		}
		
		return false;
	}
        
	// this engine's syntax to cast value as characters
	function to_char($col){
		return "to_char('".$col."')";
	}
	
	// given a column name and a value, return the syntax for NVL (if the column is null return the value)
	function nvl($col, $val){
	   return "nvl(".$col.", ".$this->dbize($val).")";
	}
	
	// save_query_result_to_csv: take a query and a filename, run the query and put the result into the file with that filename
	function save_query_result_to_csv($query, $params, $filename, $headings=true){
		
		if(file_exists($filename) && is_writable($filename)){
			$fp = fopen($filename, 'w');
		
			$res = $this->get($query, 'row', $params);

			foreach($res as $row){
						
				// Header row
				if ($headings && !isset($headerrow)){
					$headerrow = array_change_key_case(array_keys($row), CASE_UPPER);
					fputcsv($fp, $headerrow, ',', '"');
				}
				
				fputcsv($fp, $this->csvsafe_row($row), ',', '"');
			}
			
			fclose($fp);
			unset($fp);
			unset($res);
			flush();
			return true;
		}
		else{ // Could not write file / find file
			return false;
		}
	}
	
	// print query result as plaintext CSV
	function print_plaintext_as_csv($query, $params, $headings = true){
		ini_set('memory_limit', '256M');
		$res = $this->get($query, 'row', $params);
		if(isset($res) && !empty($res)){
			foreach($res as $row){
				
				// Header row
				if ($headings && !isset($headerrow)){
					$headerrow = array_change_key_case(array_keys($row), CASE_UPPER);
					echo implode(",", $this->csvsafe_row($headerrow));
					echo "\r\n";
				}
				
				echo implode(",", $this->csvsafe_row($row));
				echo "\r\n";
			}
			
			unset($res);
		}
	}
	
	// csvsafe_row: given a DB row, return a row in which all values are CSV safe (string or number; not object, array, or datetime)
	function csvsafe_row($row, $quotes = true){
            global $db;
			
            foreach($row as $k=>$v){
                    if($v instanceof DateTime){ // Return date as string like 19-JUN-2015
                            $row[$k] = strtoupper($v->format($this->dateFormat));
                    }
                    else if(is_array($v)){ // Return array like [1, 2, 3, 4]
                            $row[$k] = "[" . implode($v, ",") . "]";
                    }

                    // Unencode text
                    $row[$k] = $db->undbize($v);
					
					// Put in quotes to avoid breaking at , 
					if($quotes){
						$row[$k] = '"' . $row[$k] . '"';
					}
					
            }
            return $row;
	}
	
	// Print a silly html bar chart
	function rows_as_chart($dbrows,$xvaluefield,$yvaluefields,$colors=array(),$formats=array(),$showSeriesLabel=false,$showMetricLabel=false){	

		$ret = "<table border=0 width=100%>";
		$totalmax = getMaxFromAssoc($dbrows,$yvaluefields);
		$url = "";
		
		if(!empty($dbrows) && is_array($dbrows)){
			foreach($dbrows as $idx=>$dbrow){
			
					if(isset($colors[$idx])){
                                            $color = $colors[$idx];
                                        }
					else if(isset($colors[0])){
                                            $color = $colors[0];
                                        }
					else{
                                            $color = "#00F";
                                        }
					
					$countrows = 0;
					$label = "";
								
					foreach($yvaluefields as $idx=>$yvaluefield){

							$xvalue = "";
							$yvalue = 0;					
							$ret .= "<tr>";
							$xtext = "";
							
							if($idx == 0){
								$xformat = "text";
                                                                
								if(isset($formats[$xvaluefield])){
                                                                    $xformat = $formats[$xvaluefield];
                                                                }
								
								if(isset($dbrow[strtoupper($xvaluefield)])){
                                                                    $xvalue = $dbrow[strtoupper($xvaluefield)];
                                                                }
                                                                
								elseif(isset($dbrow[strtolower($xvaluefield)])){
                                                                    $xvalue = $dbrow[strtolower($xvaluefield)];
                                                                }
								
								if($xvalue != ""){
									$xtext = $xvalue;
									$url = getUrlFromString($xvalue);
									
									$ret .= "<td style='width:1px;white-space: nowrap;' rowspan='".count($yvaluefields)."'>".$xtext."</td>";
									if($showSeriesLabel){
                                                                            $label = iconToText($xvalue);
                                                                        }
								}
							}
							
							if(!empty($formats) && isset($formats[$yvaluefield])){
                                                            $format = $formats[$yvaluefield];
                                                        }
							else{
                                                            $format = "number";
                                                        }
							
							if($format == "percent"){
                                                            $max = 1;
                                                        }
							else{
                                                            $max = $totalmax;
                                                        }
							
							if(isset($dbrow[strtoupper($yvaluefield)])){
                                                            $yvalue = $dbrow[strtoupper($yvaluefield)];
                                                        }
							elseif(isset($dbrow[strtolower($yvaluefield)])){
                                                            $yvalue = $dbrow[strtolower($yvaluefield)];
                                                        }
							
							if($showMetricLabel){
                                                            $label = formatTag($yvaluefield);
                                                        }
							
							if($xvaluefield != $yvaluefield && isset($yvalue) && is_numeric($yvalue)){
								$color = alterColor($color,$countrows,'dark');
								$ret .= "<td>".$this->value_as_bar($yvalue, $max, $color, $format, $label, $url)."</td>";
								$countrows++;
							}
							
							$ret .= "</tr>";
					}
			}
		}
		
		$ret .= "</table>";
		
		return $ret;
	}
	
	// Support for silly html bar chart. max is the maximum value, always 1 for percents.
	function value_as_bar($val,$max=1,$color='#00F',$format="number",$label="",$url=""){
		if($max == 0 || $format == 'percent'){
                    $max = 1;
                }

		$percent = formatDecimalAsPercent($val/$max);
		
		if($format=="percent"){
                    $printvalue = $percent."%";
                }
		else if($format=="dollars"){
                    $printvalue = "$".formatLargeNumberShorthand($val);
                }
		else{
                    $printvalue = formatLargeNumberShorthand($val);
                }
                
		$printvalue = "<strong>".$printvalue."</strong>";
		
		if($label != ""){
                    $printvalue = $label." ".$printvalue;
                }
		
		$ret = "";
		
		if($url != "") { 
                    $ret .= "<a href='".$url."'>";
                }
		
                $ret .= "<table border=0 width=100% cellpadding=2px cellspacing=0 style='width;100%;border:none;padding:0.25em;margin:0'>";
			
                    $ret .= "<tr>";
					
                        $ret .= "<td width='".max(1,$percent)."%' bgcolor='".$color."' align='right' style='text-align:right'><font color='#FFF'>";
                            $ret .= $printvalue;
			$ret .= "</font></td>";
					
			$ret .= "<td";
                            
                        if($format == 'percent'){
                                $ret .= " bgcolor='#DDD'";
                            }
                            
                            $ret .= ">";
			
                        $ret .= "</td>";
					
                    $ret .= "</tr>";
		
		$ret .= "</table>";
		
                if($url != ""){
                    $ret .= "</a>";
                }
		
		return $ret;
	}

	// formatQuery -- shows a query in a visually appealing format
	function format_query($query){
		$query = str_replace("SELECT","SELECT<br>&nbsp;&nbsp;&nbsp;&nbsp;",$query);
		$query = str_replace("FROM","<br>FROM<br>&nbsp;&nbsp;&nbsp;&nbsp;",$query);
		$query = str_replace("WHERE","<br>WHERE<br>&nbsp;&nbsp;&nbsp;&nbsp;",$query);
		$query = str_replace("AND","<br>&nbsp;&nbsp;&nbsp;&nbsp;AND ",$query);
		$query = str_replace("GROUP BY","<br>GROUP BY<br>&nbsp;&nbsp;&nbsp;&nbsp;",$query);	
		$query = str_replace("ORDER BY","<br>ORDER BY<br>&nbsp;&nbsp;&nbsp;&nbsp;",$query);	
		$query = $query ."<br><br>";
		return $query;
	}
	
	// Icon to text: take a complex HTML tag and return the stripped tag text, if possible, or title, if it exists.
	public function icon_to_text($html){
		$ret = "";
		$ret = strip_tags($html);

		if($ret == ""){
			$p = preg_match('/(title|TITLE)=["\'](.+)["\']/', $html, $matches);
			if ($p > 0) {
				$ret = substr($matches[0],7,strlen($matches[0])-8);
			}
		}
		
		return $ret;
	}
	
	// Given a db result as rows, 
	// return an associative array of outer (first) column value => array of keys from the other row(s); the value is always true
	// Example:
	// APP=>40, GROUP=>1
	// APP=>41, GROUP=>2
	// APP=>41, GROUP=>3
	// APP=>42, GROUP=>3
	// Returns:
	// 40 => (1=>true)
	// 41 => (2=>true, 3=>true)
	// 42 => (3=>true)
	function rows_to_assoc($rows, $outer_key=""){
		$newarr = array();

		if(!empty($rows)){
			foreach($rows as $row){
				if($outer_key != "" && isset($row[$outer_key])){
                                    $outer_val = $row[$outer_key];
                                }
				else{
                                    $outer_val = array_pop($row);
                                }
				
				foreach($row as $cellname=>$cell){
					if($outer_key != "" && $outer_key == $cellname){
                                            continue;
                                        }
                                        
					$newarr[$outer_val][$cell] = true;
				}
			}
		}
		
		return $newarr;
	}
}

?>