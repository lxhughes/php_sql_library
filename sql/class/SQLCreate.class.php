<?php

class SQLCreate extends SQLQuery {
	
	public $primaryKeyField = "";
    public $unique_constraints = array(); // Array of constraint name => array of field names to include in that constraint
	
	// Constructor
	// columns should be format: colname => coltype
	public function __construct($table, $columns, $primaryKeyField = ""){
		$this->table = $table;
		$this->columns = $columns;
		$this->primaryKeyField = $primaryKeyField;
	}
	
	// Create table query
	public function build_query(){
		global $db;
		
		// Check for existing table
		$tab = $db->get("SELECT 1 FROM ".$this->table);
		if($tab == NULL || !isset($tab[1])){
			//if($this->debug) echo "TABLE does not exist, continue";
		}
		else{
			if($this->debug) echo $this->table . " already exists; checking for new columns... ";
			$this->query = $db->add_columns_query($this->columns, $this->table, $this->schema);
			return $this->query;
		}
			
		if(!empty($this->columns)){          
			$this->query = $db->create_query($this->columns, $this->table_with_schema(), $this->unique_constraints, $this->primaryKeyField);
			return $this->query;
		}
		
		return "";
	}
	
	// Cleanup after execution
	public function post_execute(){
		if($this->primaryKeyField != ""){
			primary_key_cleanup($this->table);
		}
	}

}

?>