<?php
require_once("Engine.class.php");

class EnginePostgres extends Engine {
	public $type = "Postgres";
	public $datatypes = array(
		"bool"=>"int",
		"datetime"=>"datetime",
		"int"=>"int",
		"float"=>"float",
		"text"=>"varchar",
		"intl_text"=>"varchar"
	);
	public $placeholderRegexp = "/\\$[0-9]+/";
	
	public function connect($params = array()){
		$connectstring = "";
		if(isset($params["server"]) && $params["server"] != "") $connectstring .= "host=" . $params["server"] . " ";
		if(isset($params["port"]) && $params["port"] != "") $connectstring .= "port=" . $params["posrt"] . " ";
		if(isset($params["database"]) && $params["database"] != "") $connectstring .= "dbname=" . $params["database"] . " ";
		if(isset($params["username"]) && $params["username"] != "") $connectstring .= "user=" . $params["username"] . " ";
		if(isset($params["password"]) && $params["password"] != "") $connectstring .= " password=" . $params["password"];
		$conn = pg_connect($connectstring);
        
        $this->connect_shared($params);
		
		return $conn;
	}
	
	public function get_stmt($query, $bind_variables){
		global $conn;	
			
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});
		
		// Prepare query and return resource
		if(!empty($bind_variables)){
			$res = pg_query_params($conn, $query, $bind_variables);
		}
		else{
			$res = pg_query($conn, $query);
		}
		
		// If resource is false, print last error
		if(!$res){
			$this->errors[] = pg_last_error($conn);
		}
		
		return $res;
	}
	
	public function get($query, $column_or_row = 'column', $bind_variables = array(), $pgconn = false){
		global $conn;
		
		$res = $this->get_stmt($query, $bind_variables);
		
		// Fetch resource into array as rows or columns
		if(!$res){ // No resource returned
			return false;
		}
		else{
			if($column_or_row == 'row'){
				$ret = pg_fetch_all($res);
			}
			else{
				$numfields = pg_num_fields($res);
				for($i=0;$i<$numfields;$i++){
					$fieldname = pg_field_name($res, $i);
					$ret[$fieldname] = pg_fetch_all_columns($res, $i);
				}
			}
		}
		
		return $ret;
	}
	
	public function execute($query, $bind_variables = array()){
		global $conn;

		// Prepare query and return resource
		try{
			$res = $this->get_stmt($query, $bind_variables);
			return $res;
		}
		catch(Exception $e){
			throw new ErrorException(pg_last_error($conn));
		}
		
	}
	
	// Typecast
	public function typecast($col, $type){
		if($type == 'character varying'){
			$col .= "::varchar";
		}
		else if($type == 'integer'){
			$col .= "::int";
		}
		
		var_dump($col);
		
		return $col;
	}
	
	// Dbize the date
	public function dbize_date($value){
		$timestamp = validate_timestamp($value);		
		$str = date('Y-m-d H:i:s', $timestamp);	
		return "'" . $str . "'::timestamp";		
	}
	
	// Return a string with date math
	// Postgres uses the intervalstring
	// (timestamp, +, '1 month', 28) yields "(timestamp + INTERVAL '1 month')"
	function datemath($date, $operator = "+", $intervalstring = "1 day", $numdays = 1){
		return "(" . $date . " " . $operator . " INTERVAL '" . $intervalstring . "')";
	}
	
	// Create placeholder for Oracle is a :strings
	// UNTESTED
	public function createPlaceholder($count){
		return "$".$count;
	}
	
	public function tables($schema=""){
		$tablestable = "information_schema.tables";
		
		if($schema != ""){
			$tablestable = $schema . "." . $tablestable;
		}
                
		$ret = $this->get("SELECT table_name FROM " . $tablestable, "column");
		return $ret;
	}	
	
	public function columns($table, $schema=""){
		$columnstable = "information_schema.columns";
		$query = "SELECT column_name FROM " . $columnstable . " WHERE table_name = " . $this->dbize(strtoupper($table));
		
		if($schema != ""){
			$query .= " AND table_schema = " . $this->dbize(strtoupper($schema));
		}
		
		$ret = $this->get($query, "column");
		return $ret["column_name"];
	}
	
	public function column_datatype($column, $table, $schema=""){
		$columnstable = "information_schema.columns";
		
		// Split off schema from table if it's there
		$tableparts = explode(".", $table);
		$table = $tableparts[count($tableparts)-1];
		if($tableparts[0] != $table){
			$schema = $tableparts[0];
		}
		
		$query = "SELECT data_type FROM " . $columnstable . " WHERE column_name = ".$this->dbize(strtolower($column))." AND table_name =" . $this->dbize(strtolower($table));
		
		if($schema != ""){
			$query .= " AND table_schema = " . $this->dbize(strtolower($schema));
		}
			
		$ret = $this->get($query, "column");

		if(empty($ret)) return "";
		return $ret["data_type"];
	}
	
	public function column_datatypes($table, $schema=""){
		$columnstable = "information_schema.columns";
		
		$query = "SELECT column_name, data_type FROM " . $columnstable . " WHERE table_name =" . $this->dbize(strtolower($table));
		
		if($schema != ""){
			$query .= " AND table_schema = " . $this->dbize(strtolower($schema));
		}
		
		$arrs = $this->get($query, "column");

        $ret = array();
        
        if(isset($arrs['column_name']) && isset($arrs['data_type'])){
            $cols = $arrs['column_name'];
            $types = $arrs['data_type'];

            if(!empty($cols) && !empty($types)){
                foreach($cols as $idx=>$col){
                    $ret[$col] = $types[$idx];
                }
            }
        }
			
		return $ret;	
	}
	
		// save_query_result_to_csv: take a query and a filename, run the query and put the result into the file with that filename
	function save_query_result_to_csv($query, $params=array(), $filename="downloads/export.csv", $headings=true){
		
		$fp = fopen($filename, 'w');
		
		$stmt = $this->get_stmt($query, true);

		if($stmt){
		
			while ($row = pg_fetch_assoc($stmt)){

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