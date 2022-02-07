<?php

class SQLQuery {
	
	public $table; 							// String: table you are querying. Required.
	public $schema = "";				    // Schema of the table. 
	public $wherefields = array();			// Array of strings: the where clauses. examples: "fact_visit.transaction_time > 20", "dim_category.id NOT IN(1, 2, 3)"
	public $wherearray = array();			// Optional array of key=>values to be turned into equal wherefields. e.g. ("cake"=>5) yields 'where cake = 5'. Value could be array e.g. ("cake"=>array(5,6,7)) yields 'where cake IN(5,6,7)'
	public $whereoperators = array();		// Optional array to complement wherearray, in case of operations other than equals. Key must match key in wherearray. So if wherearray contains ("cake"=>5) and whereoperators contains ("cake"=>">") then the wherestring would yield "cake > 5"
	public $columns = array(); 				// Array of columns in the form column_name=>data_type
	public $query = "";						// The query -- update with build_query. This could be a string or an array.
	public $debug = false;					// DEBUG mode (prints info & errors to screen)
	public $international = false;			// Whether this table holds international text (e.g. Chinese characters)
	public $withs = array();				// Prepend 'with' queries; in form tag => query
	public $tablealias = "";				// Alias for table for use in JOIN queries
	public $bind_variables = array();		// Replace all ?'s in the query with these in order. Created automatically for wherearray
	public $startBindNum = 0;				// Startnum for bind variables (actually 1 down from actual start num)
	public $limit = 0;						// Limit of results to return (for select queries)
	
	// Constructor
	public function __construct($table){
		$this->table = $table;
	}
	
	// Return the query
	// Alias this in the child
	public function build_query(){
	}
	
	// Perform cleanup after executing the query
	// Alias this in the child, if needed (or leave it blank)
	public function post_execute(){
		return true;
	}
	
	
	// Generalized execute query function
    // If the query is an array, run all the queries
	public function execute(){
		global $db;
		$this->build_query();
		
		try{
		
			if(is_array($this->query) && !empty($this->query)){
				$ret = array();
				foreach($this->query as $query){
					$ret[] = $db->execute($query, $this->bind_variables);
					$this->post_execute();
				}
				
				return $ret;
			}
			if($this->query != ""){
				$ret = $db->execute($this->query, $this->bind_variables);
				$this->post_execute();
				
				return $ret;
			}
		}
		catch(Exception $e){
			$exceptionMessage = $e->getMessage() . " \r\n I used this query: " . $this->print_query();
			throw new Exception($exceptionMessage);
		}
		
		return false;
	}
	
	// Table with schema, if appropriate
	public function table_with_schema(){
		$string = "";
		
		if($this->schema != "") $string = $this->schema . ".";
		$string .= $this->table;
		
		$string .= " AS ".$this->get_table_alias();
		
		return $string;
	}
	
	// Return an array of columns in the form column_name=>data_type, also update $this->columns
	public function get_columns(){
		global $db;

		if(!empty($this->columns)) return $this->columns;
		if(!isset($this->table)) return false; // No table set
		
		$dbcol = $db->column_datatypes($this->table);
                $this->columns = $dbcol;
		
		if(!$dbcol){
			return array();
		}
        
        return $this->columns;
	}
	
	// Given an array of columns in the form column_name=>data_type, update $this->columns
	public function set_columns($columns){
		$this->columns = $columns;
	}
	
	// Get the data type of a particular column
	public function get_column_datatype($key){
		$datatype = "";
		if(isset($this->columns[strtoupper($key)])){
			$datatype = $this->columns[strtoupper($key)];
		}
		else if(isset($this->columns[strtolower($key)])){
			$datatype = $this->columns[strtolower($key)];
		}
		return strtolower($datatype);
	}
        
        public function get_column_datatypes(){
            $datatypes = array();
            
            foreach($this->columns as $col){
                $datatypes[$col] = $this->get_column_datatype[$col];
            }
            return $datatypes;
        }
	
	// Associate the values with columns
	public function clean_values($values){
		global $db;
		
		$clean_values = array();
		$values = array_change_key_case($values);
		
		$columns = $this->get_columns();

		if(!empty($columns)){
			$this->columns = $columns;
			
			foreach($this->columns as $ukey=>$datatype){
				
				$key = strtolower($ukey);
				if(isset($values[$key]) && (get_class($this) == 'SQLUpdate' ||  ($values[$key] != "" && $values[$key] != null))){ // Don't insert null values. (Note: The 'update' version of this SHOULD insert nulls, for overwriting.)
					
					if(is_array($values[$key])){
						foreach($values[$key] as  $sk=>$sv){
							$clean_values[$key][$sk] = $db->dbize($sv);
						}
					}
					else{
						$clean_values[$key] = $db->dbize($values[$key], $datatype, false);
					}
				}
			}
		}
		else {
			$clean_values = $values;
		}
		
		$this->values = $clean_values;
		return $clean_values;
	}
	
	// Add values
	public function add_values($values){
		
		$this->values = $this->clean_values($values);
		
		return $this->values;
	}
	
	// Add a single value
	public function add_value($key, $value){
		$columns = $this->get_columns();
		if(isset($columns[strtoupper($key)]) || isset($columns[strtolower($key)])){
			$this->values[$key] = $value;
		}
	}
	
	// Create where clause out of where fields
	public function wherefields_to_string(){
		global $db;
		$wherestring = "";
		$columns = $this->get_columns();
		
		// Do Wherefields first for ease of hacking bind variables
		if(!empty($this->wherefields) && is_array($this->wherefields)){
			foreach($this->wherefields as $filter){
				if($wherestring != "") $wherestring .= " AND ";
				$wherestring .= $filter;
			}
		}
		
		// Add limit to wherearray for Oracle only
		// UNTESTED
		if(is_integer($this->limit) && $this->limit > 0 && $db->type == "Oracle"){
			$wherefields[] = "rownum <= ".$limit;
		}
		
		// Do WhereArray
		$bvct = $this->startBindNum;

		if(!empty($this->bind_variables)){
			$bvct += count($this->bind_variables);
		}
		
		if(!empty($this->wherearray) && is_array($this->wherearray)){
			foreach($this->wherearray as $k=>$v){

				$datatype = "";
				if(isset($columns[strtoupper($k)])){
					$datatype = $columns[strtoupper($k)];
				}

				if($wherestring != "") $wherestring .= " AND ";
				
				if(is_array($v)){ // $v is array; k IN($v1, $v2, $v3) etc with bind variables
					
					$qms = array();
					
					foreach($v as $subv){
						$bvct++; 
						$qms[] = $db->createPlaceholder($bvct);
						$this->bind_variables[] = $db->dbize($subv, $datatype, false);
					}
				
					$wherestring .= $this->column_with_tablealias($k, $datatype) . ' IN(' . implode(",", $qms) . ')';
				}
				
				else if(strtoupper($v) == "NULL" || strtoupper($v) == "NOT NULL"){ // special for NULL
					$wherestring .= $this->column_with_tablealias($k, $datatype) . ' IS ' . strtoupper($v);
				}
				else{ // $v is single value; basic k = v with bind variables
					$bvct++; 
					
					$operator = '=';
					if(!empty($this->whereoperators) && isset($this->whereoperators[$k])){
						$operator = $this->whereoperators[$k];
					}
					
					$wherestring .= $this->column_with_tablealias($k, $datatype) . ' ' . $operator . ' ' . $db->createPlaceholder($bvct);
					$this->bind_variables[] = $db->dbize($v, $datatype, false);
				}
			}
		}
		
		if($wherestring != "") $wherestring = " WHERE ".$wherestring;
		
		return $wherestring;
	}
		
	// Take an array of potential table rows, create the table if needed, and insert the rows (or update if the key selectors already exist)
	public function reset_table($rows, $selectors=array(), $primary_key = ""){
		$current_cols = $this->get_columns();
		
		// The table doesn't exist: make it
		if(!$current_cols || empty($current_cols)){
			$datatypes = $this->get_column_datatypes();
			$cr = new SQLCreate($this->table,$datatypes,$primary_key);
			$cr->debug = $this->debug;
			$cr->execute();
		}
		
		if(empty($selectors)){ // No selectors: Always insert the rows
			foreach($rows as $rownum=>$row){
                            $ins = new SQLInsert($this->table,$row);
                            $ins->debug = $this->debug;
                            $ins->execute();
			}
		}
		else{ // Selectors exist: update the given rows
			foreach($rows as $rownum=>$row){
				
				$row_selectors = array();
				foreach($selectors as $selkey){
					if(isset($row[$selkey])) $row_selectors[$selkey] = $row[$selkey];
				}
				
				if(!empty($row_selectors)){
					$upd = new SQLUpdate($this->table,$row,$row_selectors,true);
					$upd->debug = $this->debug;
					$upd->execute();
				}
				else{
					$ins = new SQLInsert($this->table,$row);
					$ins->debug = $this->debug;
					$ins->execute();
				}
			}
		}
		
	}
	
	// Return a given column name as table.column_name (in case of joins)
	public function column_with_tablealias($col, $type = ""){
		global $db;
		
		$ret = $col;
					
		if($type != ""){
			$col = $db->typecast($col, $type);
		}
		
		if(!strpos($col, ".") // doesn't already have a .
			&& !is_numeric($col) // col isn't a number_format
			&& $col != "NULL" // col isn't NULL
			&& isset($this->tablealias) // table alias exists
			&& $this->tablealias != "" // table alias isn't empty
			&& !empty($this->joins) // there are joins
		){
				$ret = $this->tablealias . "." . $col;
		}

		return $ret;
	}
	
	// Get the table alias for this table. You can set it, or it will default to the table name, as long as that's not a reserved SQL keyword. If so, it will use the 1st letter of the table name.
	public function get_table_alias(){
		global $db;
		
		$sql_reserved_words = array();
		if(isset($db->sql_reserved_words) && !empty($db->sql_reserved_words)){
			$sql_reserved_words = $db->sql_reserved_words;
		}
				
		if($this->tablealias != ""){
			$tablealias = $this->tablealias;
		}
		else if(in_array($this->table, $sql_reserved_words)){
			$tablealias = $this->table[0];
			$this->tablealias = $tablealias;
		}
		else{
			$tablealias = $this->table;
			$this->tablealias = $tablealias;
		}

		return $tablealias;
	}
	
	// Print query with bind variables inserted for testing (i.e. to cut and paste into PSQL)
	public function print_query(){
		global $db;
		
		if(!isset($this->query) || $this->query == ""){
			$this->query = $this->build_query();
		}
		
		$replacements = $this->bind_variables;
		return preg_replace_callback($db->placeholderRegexp, function($matches) use (&$replacements) {
			global $db;
			$subin = array_shift($replacements);
			$subin = $db->dbize($subin);
			return $subin;
		}, $this->query);
	}

}

?>