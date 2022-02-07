<?php

class SQLDelete extends SQLQuery {
    
    public $selectors = array();	// Values in the format column_name => value to be used to select the correct row. You can also use wherefields (array of phrases e.g. 'numcats > 10')
    
    // Constructor
    public function __construct($table, $selectors=array()){
         $this->table = $table;
	 $this->selectors = $selectors;
    }
    
    // Create query
    public function build_query(){
		global $db;
       
        // Add selectors to WHERE clause
	foreach($this->selectors as $key=>$value){
		$this->wherefields[] = "$key = " . $db->dbize($value,$this->get_column_datatype($key));
	}
        
        $wherestring = $this->wherefields_to_string();
        
        $query = "DELETE FROM " . $this->table_with_schema() . $wherestring;
        $this->query = $query;

        return $this->query;
    }
}
