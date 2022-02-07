<?php

require_once("SQLQuery.class.php");

class SQLUpdate extends SQLQuery {
	
	public $values = array(); 		// Values in the format column_name => value to be updated. 
	public $selectors = array();	// Values in the format column_name => value to be used to select the correct row. You can also use wherefields (array of phrases e.g. 'numcats > 10')
	public $insertIfAbsent = false;	// Whether to INSERT a row that does not already exist in the table per the selectors
    public $multiSelectors = array(); // If this is a multiple insert, this is an array of selector arrays which should be parallel to the array of values (which is also arrays)
	
	// Constructor
	public function __construct($table, $values=array(), $selectors=array(), $insertIfAbsent=false){
		$this->table = $table;
		$this->values = $this->add_values($values);
		$this->selectors = $selectors;
		$this->insertIfAbsent = $insertIfAbsent;
	}
	
	// Create table query
	public function build_query(){
        global $db; 
        
		$setstring = "";
		
		// If insertIfAbsent is set, check for the row
		if($this->insertIfAbsent){
			$sel = new SQLSelect($this->table, $this->schema);
			$sel->selectfields[] = "count(*) CT";
			$sel->wherearray = $this->selectors;
			$val = $sel->execute();
			
			if($val == NULL || empty($val) || $val["ct"][0] === "0"){
				
				//echo "<br>This column does not exist. " . $sel->query;
				
				$values = array_merge($this->values, $this->selectors);
				
				$ins = new SQLInsert($this->table,$values);
				$ins->schema = $this->schema; 
				$ins->build_query();				
				$this->query = $ins->query;
				$this->bind_variables = $ins->bind_variables;
				
				return $this->query;
			}
			//else{
				//echo "<br>This column exists. " . $sel->query . " " . var_export($val, true);
			//}
		}
		
		// Find placeholder next
		$placeholderNext = max(1, $this->startBindNum + count($this->bind_variables));
        
        // If this is a multi update (it has multi update selectors & the values are an array of arrays) do a multi update
        if(!empty($this->multiSelectors) && is_array($this->multiSelectors[0]) && is_array($this->values[0])){
            $this->query = $db->multi_update_query($this->table_with_schema(), $this->values, $this->multiSelectors, $this->wherefields);
			$this->bind_variables = array_merge(array_values($this->bind_variables), array_values($this->values));
			foreach($this->multiSelectors as $selectors){
				$this->bind_variables = array_merge(array_values($this->bind_variables), array_values($selectors));
			}
        }
        else{ // Not a multi update - a regular update 
            $this->query = $db->update_query($this->table, $this->values, $this->selectors, array(), $this->schema, $placeholderNext);
			$this->bind_variables = array_merge(array_values($this->bind_variables), array_values($this->values));
			$this->bind_variables = array_merge(array_values($this->bind_variables), array_values($this->selectors));
        }
		
		return $this->query;
	}

}

?>