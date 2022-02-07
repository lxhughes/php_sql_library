<?php
require_once("Engine.class.php");

class EngineSQLServer extends Engine {
	public $type = "SQLServer";
	public $datatypes = array(
		"bool"=>"int",
		"datetime"=>"datetime",
		"int"=>"int",
		"float"=>"float",
		"text"=>"varchar",
		"intl_text"=>"nvarchar"
	);	
	
	public function connect($params = array()){
		if(isset($params["instance"]) && $params["instance"] != ""){
                    $params["server"] .= "\\" . $params["instance"];
                }
		
		$connectionInfo = array( "Database"=>$params["database"], "UID" => $params["username"], "PWD" => $params["password"]);
		$conn = sqlsrv_connect( $params["server"], $connectionInfo);

		if(!$conn){
			trigger_error("Connection could not be established! ".var_export(sqlsrv_errors(), true));
		}
           
        $this->connect_shared($params);
		
		return $conn;
	}
	
	public function get($query, $fetchas = "assoc"){
		$ret = false;
		if($fetchas == "numeric" || $fetchas == "column"){
                    $fetchasval = SQLSRV_FETCH_NUMERIC;
                }
		else{
                    $fetchasval = SQLSRV_FETCH_ASSOC;
                }
		
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try{
			$stmt = $this->query($query);
			if($stmt){
				$ret = array();
				
				while ($row = sqlsrv_fetch_array($stmt, $fetchasval)){
					if($fetchas == "column"){
						$ret[] = $row[0];
					}
					else{
						$ret[] = $row;
					}
				}
			}
			
			return $ret;
		}
		catch(Exception $e){
			//
		}

		restore_error_handler();
		
	}
	
	public function execute($query, $params = array()){
		$ret = false;

		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try{
			$stmt = $this->prepare($query, $params);
			if($stmt) {
				$ret = sqlsrv_execute($stmt);
			}
			if(!$ret){
				trigger_error(var_export(sqlsrv_errors(), true));
				return false;
			}
			return $ret;
		}
		catch(Exception $e){
			//
		}

		restore_error_handler();	

	}
	
	public function prepare($query, $params = array()){
		global $conn;
		
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try{
			$ret = sqlsrv_prepare($conn, $query, $params);
			if(!$ret){
				error_out(var_export(sqlsrv_errors(), true));
			}
			return $ret;
		}
		catch(Exception $e){
			//
		}

		restore_error_handler();
	}
	
	// Perform query and return STATEMENT
	public function query($query, $params = array()){
		global $conn;
	
		set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext) {
			// error was suppressed with the @-operator
			if (0 === error_reporting()) {
				return false;
			}

			throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
		});

		try{
			$ret = sqlsrv_query($conn, $query);
			if(!$ret){
				error_out(var_export(sqlsrv_errors(), true));
			}
			return $ret;
		}
		catch(Exception $e){
			//
		}

		restore_error_handler();
	}
	
	public function tables($schema = ""){
		$ret = db_get("SELECT table_name FROM information_schema.tables","numeric");
		return $ret;
	}
	
	public function columns($table, $schema = ""){
		$ret = db_get("SELECT * FROM information_schema.columns WHERE table_name =" . $this->dbize(strtoupper($table)), "column");
		return $ret;
	}
		
	public function dbize_date($value){
		return "Convert(datetime, '".$value."')";
	}
	
	public function primary_key_clause_field($fieldname){
		$clause = $fieldname." INT IDENTITY(1,1) PRIMARY KEY";				
		return $clause;
	}

	// Given a field (and optional table), return the LISTAGG syntax (to return a comma separated list)
	// UNTESTED ALSO HORRIBLE	
	public function listagg_field($field, $table){
		$ret = "STUFF(( SELECT ',' " . $field . " FROM " . $table . " a WHERE ".$table . "." . $field . " = a." . $field . " FOR XML PATH('')), 1, 1, '')"; 
		return $ret;
	}

	function save_query_result_to_csv($query, $params=array(), $filename="export.csv", $headings=true){
	
		$fp = fopen($filename, 'w');
	
		$stmt = $this->query($query, $params);

		if($stmt){		
			while ($row = sqlsrv_fetch_array($stmt, SQLSRV_FETCH_ASSOC)){
						
				// Header row
				if ($headings && !isset($headerrow)){
					$headerrow = array_change_key_case(array_keys($row), CASE_UPPER);
					fputcsv($fp, $headerrow);
				}
				
				fputcsv($fp, $this->csvsafe_row($row, false));
			}
		}
		
		fclose($fp);
		unset($fp);
		unset($res);
		flush();
		return true;

	}

}
	

?>