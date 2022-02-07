<?php

class SQLJoin {
	
	public $schema1 = "";
	public $schema2 = "";
	public $table1;
	public $table2;
	public $table1code = "";
	public $table2code = "";
	public $table1field = ""; // Fields are optional b/c not used in CROSS JOIN
	public $table2field = ""; // Fields are optional b/c not used in CROSS JOIN
	public $jointype = "LEFT";
	public $table1alias;
	public $table2alias;
	
	// table1 = other, table2 = this
	public function __construct($table1field, $table1, $schema1, $table2field, $table2, $schema2="", $jointype="LEFT"){
				
		$this->table1field = $table1field;
		$this->table2field = $table2field;
		$this->table1 = $table1;
		$this->table2 = $table2;
		if($schema1 != "") $this->schema1 = $schema1;
		if($schema2 != "") $this->schema2 = $schema2;
		else $this->schema2 = $this->schema1;
		$this->jointype = $jointype;
		$this->table1alias = $table1;
		$this->table2alias = $table2;
	}
	
	// giventable = current table
	// A = other, B = this
	public function printjoin($giventable = "") {
		$tableA = $this->table1;
		$fulltableA = $this->table1;
		$tableAalias = $this->table1alias;
		if($this->schema1 != "") $fulltableA = $this->schema1.".".$this->array_or_string_value($this->table1,0);
		$tableAfield = $this->table1field;
		
		$tableB = $this->table2;
		$fulltableB = $this->table2;
		$tableBalias = $this->table2alias;
		if($this->schema2 != "") $fulltableB = $this->schema2.".".$this->array_or_string_value($this->table2,0);
		$tableBfield = $this->table2field;	
		
		if(is_array($tableAfield) && is_array($tableBfield)){
			$fieldstring = "";
			for($i=0; $i<count($tableAfield);$i++){
				if($fieldstring != "") $fieldstring .= " AND ";
				
				if(!strpos($tableBfield[$i], '.')) $fieldstring .= $this->array_or_string_value($tableB, $i).".";
				
				$fieldstring .= $tableBfield[$i]." = ";
				
				if(!strpos($tableAfield[$i], '.')) $this->array_or_string_value($tableA, $i).".";
				
				$fieldstring .= $tableAfield[$i];
			}
			
			$printline = " ".$this->jointype." JOIN ".$fulltableA." AS ".$tableAalias;
			
			if($this->jointype != "CROSS"){
				$printline .= " ON ".$fieldstring;
			}
		}
		else{
			$printline = " ".$this->jointype." JOIN ".$fulltableA." AS ".$tableAalias;
			
			if($this->jointype != "CROSS"){
				$printline .= " ON ";
			
				if(!strpos($tableBfield, '.')) $printline .= $tableBalias.".";
				
				$printline .= $tableBfield." = ";
				
				if(!strpos($tableAfield, '.'))  $printline .= $tableAalias.".";
				
				$printline .= $tableAfield;
			}
		}

		return $printline;
	}
	
	// Given a value and an index, returns the value (if it's a string) or the value at the index (if it's an array)
	public function array_or_string_value($val, $idx){
		if(is_array($val)){
			return $val[$idx];
		}
		else return $val;
	}
	
}

?>