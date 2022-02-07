<?php

class SQLInsert extends SQLQuery {
	
	public $values = array(); 		// Values in the format column_name => value
	
	// Constructor
	public function __construct($table, $values=array(), $createIfNeeded=false){
		$this->table = $table;	
		
		$this->values = $this->add_values($values);

		if($createIfNeeded){
			$cols = $this->get_columns();
			
			if(!$cols){
				$create = new SQLCreate($table, $this->row_to_datatypes($values));
				$create->execute();
			}
			
		}
		
	}
	
	// Create table query
	public function build_query(){
		global $db;
		$this->query = $db->insert_query($this->values, $this->table_with_schema());
		$this->bind_variables = array_values($this->values);
		return $this->query;
	}

}

?>