<?php
require_once("Engine.class.php");

class EngineOracle extends Engine {
	public $type = "Oracle";
	public $datatypes = array(
		"bool"=>"int",
		"datetime"=>"datetime",
		"int"=>"int",
		"float"=>"float",
		"text"=>"varchar2",
		"intl_text"=>"nvarchar2"
	);
	public $dateFormat = 'd-M-Y';
	public $placeholderRegexp = "/(\\:[A-Za-z]+)/";
	
	public function connect($params = array()){
		$conn = @oci_connect($params["username"], $params["password"], $params["server"], 'UTF8');
		if (!$conn) {
			$e = oci_error();
			trigger_error(htmlentities($e['message'], ENT_QUOTES, 'UTF-8'), E_USER_ERROR);
		}
        
        $this->connect_shared($params);
		
		return $conn;
	}
	
	public function get($query, $column_or_row = 'column', $orconn = false){
		if(!$orconn){
			global $conn;
			$orconn = $conn;
		}
		
		if($column_or_row == 'row'){
			$fetchas = OCI_FETCHSTATEMENT_BY_ROW;
		}
		else if($column_or_row == 'column'){
			$fetchas = OCI_FETCHSTATEMENT_BY_COLUMN;
		}
		else{
			$fetchas = OCI_ASSOC + OCI_RETURN_NULLS;
		}
		
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try{
			$stid = oci_parse($orconn, $query);
			if($stid){
				oci_execute($stid);
		
				$nrows = oci_fetch_all($stid, $res, null, null, $fetchas);
		
				return $res;
			}
		}
		catch(Exception $e){
			$this->errors[] = $e->getMessage();
		}

		restore_error_handler();
	}
	
	public function parse($query){
		global $conn;
		
		return oci_parse($conn, $query);
	}
	
	public function execute($query, $params=array()){
		global $conn;
	
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try {	
			$stid = oci_parse($conn, $query);
			
			// THIS BLOCK NOT TESTED
			if(!empty($params)){
				
				$placeholders = $this->getPlaceholdersFromQuery($query);
				
				foreach($params as $idx=>$param){
					$next_placeholder = $placeholders[$idx];
					oci_bind_by_name($stid, $next_placeholder, $param, strlen($param));
				}
			}
			// END UNTESTED
			
			$ret = oci_execute($stid);
			if($ret == true && $return_stid == true){
				$ret = $stid;
			}
		}
		catch(Exception $e){
			$this->errors[] = $e->getMessage();
			$ret = false;
		}
		
		return $ret;
	}
	
	// Only for Oracle
	public function get_stmt($query){
		global $conn;
	
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try {	
			$stid = oci_parse($conn, $query);
			return $stid;
		}
		catch(Exception $e){
			$this->errors[] = $e->getMessage();
			return false;
		}
	}
	
	// Create placeholder for Oracle is a :strings
	// UNTESTED
	public function createPlaceholder($count){
		return ":".$count;
	}
	
	public function tables($schema=""){
		$tablestable = "user_tables";
		
                if($schema != ""){
                    $tablestable = $schema . "." . $tablestable;
                }
                
		$ret = $this->get("SELECT table_name FROM " . $tablestable, "column");
		return $ret;
	}
	
	public function columns($table, $schema=""){
		$columnstable = "user_tab_columns";
		$query = "SELECT column_name FROM " . $columnstable . " WHERE table_name =" . $this->dbize(strtoupper($table), "column");
		$ret = $this->get($query, "column");
		return $ret["COLUMN_NAME"];
	}
	
	public function column_datatype($column, $table, $schema=""){
		$columnstable = "user_tab_columns";
		
		// Remove schema from table if it's there
		$tableparts = explode(".", $table);
		$table = $tableparts[count($tableparts)-1];
		
		$query = "SELECT data_type FROM " . $columnstable . " WHERE column_name = ".$this->dbize(strtoupper($column))." AND table_name =" . $this->dbize(strtoupper($table));
		$ret = $this->get($query, "column");

		if(empty($ret)) return "";
		return $ret["DATA_TYPE"][0];
	}
	
	public function column_datatypes($table, $schema=""){
		$columnstable = "user_tab_columns";
		$query = "SELECT column_name, data_type FROM " . $columnstable . " WHERE table_name =" . $this->dbize(strtoupper($table));
		$arrs = $this->get($query, "column");

        $ret = array();
        
        if(isset($arrs['COLUMN_NAME']) && isset($arrs['DATA_TYPE'])){
            $cols = $arrs['COLUMN_NAME'];
            $types = $arrs['DATA_TYPE'];

            if(!empty($cols) && !empty($types)){
                foreach($cols as $idx=>$col){
                    $ret[$col] = $types[$idx];
                }
            }
        }
			
		return $ret;	
	}
	
	// Support for insert_query
	// Given array of arrays of key=>value & table, return multiple insert query
	public function multiple_insert_query($values, $table){
		$clauses = $this->multiple_insert_clauses($values, $table);
			
		$query = "INSERT ALL\n";
		foreach($clauses as $clause){
			$query .= "INTO ".$table." ".$clause."\n";
		}
		$query .= "SELECT 1 FROM DUAL";
		
		return $query;
	}
	
	// Support function for MULTIPLE INSERT 
	// Given an array of arrays of key=>value, return array of strings in form:
	// ["(col1,col2) VALUES(val1,val2)","(col1,col2) VALUES(vala,valb)"]
	public function multiple_insert_clauses($values){
		$clauses = array();
		
		foreach($values as $valarr){
			$clauses[] = $this->insert_clause($valarr);
		}
		
		return $clauses;
	}

	public function is_already_dbized($value){
		if(substr($value, 0, 8) == 'to_date(') return true;
		return false;
	}
		
	public function dbize_date($value){
		$ymddate = date("Y-m-d", strtotime($value));
		return "to_date('".$ymddate."','YYYY-MM-DD')";
	}

	// Primary key clause
	public function primary_key_field($name, $datatype=""){
		if($datatype == ""){
			$datatype = "int";
		}
		return $name . " " . $datatype . " NOT NULL PRIMARY KEY";

	}

	public function auto_increment_primary_key_field($fieldname){
		$clause = "";
		
		if($this->version != "" && $this->version >= 12.3){
			$clause = $fieldname." NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY";
		}
		else{
			$clause = $fieldname." INT NOT NULL PRIMARY KEY";				
		}
		
		return $clause;
	}
	
	public function auto_increment_primary_key_cleanup($table){
	
		if($this->version != "" && $this->version < 12.3){
				
			// Create sequence
			$this->execute("CREATE SEQUENCE ".$table."_seq START WITH 1");
			
			// Create or replace the trigger
			$this->execute("CREATE OR REPLACE TRIGGER ".$table."_trig 
			BEFORE INSERT ON ".$table."
			FOR EACH ROW
			
			BEGIN
				SELECT ".$table."_seq.NEXTVAL
				INTO	:new.id
				FROM	dual;
			END;");
		}
		
		return true;		
	}
	
	public function listagg_field($field, $table=""){
		$ret = "LISTAGG(";
		
                if($table != ""){
                    $ret .= $table . "."; 
                }
                
		$ret .= $field . ", ', ') WITHIN GROUP (ORDER BY ";
		
                if($table != ""){
                    $ret .= $table . "."; 
                }
		
                $ret .= $field . ")";	
		return $ret;
	}
	
	// save_query_result_to_csv: take a query and a filename, run the query and put the result into the file with that filename
	function save_query_result_to_csv($query, $filename, $headings=true){
		
		$fp = fopen($filename, 'w');
		
		$stmt = $this->get_stmt($query, true);

		if($stmt){
		
			while ($row = oci_fetch_assoc($stmt)){

				// Header row
				if ($headings && !isset($headerrow)){
					$headerrow = array_keys($row);
					fputcsv($fp, $headerrow, ',', '"');
				}

				// Put the row
				fputcsv($fp, $this->csvsafe_row($row), ',', '"');
			}
		}
		
		fclose($fp);
	}
}

?>