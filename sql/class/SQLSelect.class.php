<?php

require_once("SQLQuery.class.php");
require_once("SQLJoin.class.php");

class SQLSelect extends SQLQuery {
	
	public $selectfields = array();			// Array of strings: the select clauses. examples: "fact_visit.id", "sum(fast_staff_session.duration)"
	public $joins = array();				// Array of 'Join' objects
	public $groupfields = array();			// Array of strings: the groupby fields. examples: "dim_date.year"
	public $havingfields = array(); 		// Array of strings: the 'having' fields in a group. examples: "count(*) > 250"
	public $orderfields = array();			// Array of strings: the orderby fields. examples: "dim_date.year"
	public $limit = 0;						// Integer; limit query results to this many (0 = no limit, return all)
	public $fetchas = "column"; 			// Fetch option for select execute
	public $rollup = false;					// Whether the rollup the groups (for grand totals)
	public $schema = "";					// Schema for selects
	private $joincount = 0;					// Count of joins on this query
	
	// Constructor
	public function __construct($table,$schema=""){
	
		$this->table = $table;
		if($schema != ""){
                    $this->schema = $schema;
                }
	}
	
	// Execute select
	public function execute($fetchas=""){
		global $db;
		
		if($fetchas != ""){
			$this->fetchas = $fetchas;
		}
		if(!isset($this->query) || $this->query == ""){
			$this->query = $this->build_query();
		}
		
		$res = $db->get($this->query, $this->fetchas, $this->bind_variables);
		return $res;
	}
	
	// Alias this with personalized prepare_query function for your own extension class
	public function prepare_query(){
	}
	
	// The basic build query
	public function build_query(){
		global $db;
		
		if($this->query != ""){
			return $this->query;
		}
		
		$joinstring = "";
		$groupstring = "";
		$orderstring = "";	
		$limitstring = "";		
		
		$this->prepare_query();
		
		// Identify the table; this is required
		if($this->table == ""){
            return false;
		}
		
		// Build the select clause.
		if(is_array($this->selectfields) && !empty($this->selectfields)){
			$select = "";
			
			foreach($this->selectfields as $alias=>$code){
				
				// Add comma to list if needed
				if($select != ""){
					$select .= ", ";
				}
				
				// Add join prefix to current field
				if(!empty($this->joins)){
					list($code, $alias) = $this->prefixSelectorWithTablenameIfApplicable($code, $alias);
				}				

				if($this->rollup && (in_array($code,$this->groupfields) || (!is_numeric($alias) && isset($this->groupfields[$alias])))){ // Turn null to TOTAL for null COLUMN value if ROLLUP
					$select .= $db->nvl($db->to_char($code),'TOTAL');
				}
				else{
					$select .= $code;
				}
				
				if(strpos($alias,' ') !== false){ // Alias which contains spaces: SELECT cake_type AS 'Cake Type'
					$select .= " as '".$alias."'";
				}
				else if(!is_numeric($alias)){ // Other alias: SELECT cake_type as cake
					$select .= " as ".$alias; 
				}
				else if($this->rollup){
					$expl = explode(".",$code);
					$homemadeAlias = $expl[count($expl)-1];
					$select .= " as ".$homemadeAlias;
				}

			}
		}
		else{
			$select = "*";
		}
		
		// Build the joins.
		if(!empty($this->joins) && is_array($this->joins)){
			foreach($this->joins as $join){
				$joinstring .= $join->printjoin($this->table);
			}
		}
		
		// Build the "where" clause.
		$wherestring = $this->wherefields_to_string();
		
		// Build the groupby clause.
		if(!empty($this->groupfields) && is_array($this->groupfields)){
	
			foreach($this->groupfields as $groupby){
				
				// Add comma if needed
				if($groupstring != "") $groupstring .= ", ";
				
				// Add join prefix to current field
				if(!empty($this->joins)){
					list($groupby, $gbalias) = $this->prefixSelectorWithTablenameIfApplicable($groupby);
				}
				
				$groupstring .= $groupby;
			}
			if($groupstring != ""){
				$groupstringfinal = " GROUP BY ";
				if($this->rollup) $groupstringfinal .= "ROLLUP(";
				$groupstringfinal .= $groupstring;
				if($this->rollup) $groupstringfinal .= ")";
				
				$groupstring = $groupstringfinal;
			}
			
			if(!empty($this->havingfields) && is_array($this->havingfields)){
				$havingstring = " HAVING ";
				$havingstring .= implode(" AND ", $this->havingfields);
				$groupstring .= $havingstring;
			}
		}
		
		// Build the orderby clause.
		if(!empty($this->orderfields) && is_array($this->orderfields)){
			foreach($this->orderfields as $orderby){
				if($orderstring != "") $orderstring .= ", ";
				$orderstring .= $orderby;
			}
			if($orderstring != "") $orderstring = " ORDER BY ".$orderstring;
		}
		
		// Build the limit clause. (not Oracle; for Oracle the limit has to be added to the WHERE)
		if(is_integer($this->limit) && $this->limit > 0 && $db->type != "Oracle"){
			$limitstring .= " LIMIT ".$this->limit;
		}

		// Build the union clause.
		$unionstring = "";
		if(isset($this->unionQuery)){

			if(is_array($this->unionQuery)){
				$ct = 0;
				foreach($this->unionQuery as $uniquery){
					$unionstring .= $this->createUnionString($uniquery, $ct);
					$ct++;
				}
			}
			else{
				$unionstring = $this->createUnionString($this->unionQuery, "");
			}
		}
		
		// Build the WITHS
		$withstring = "";
		$withstrings = array();
		if(!empty($this->withs)){
			foreach($this->withs as $tag=>$query){
				$withstrings[] = $tag . " as (" . $query . ")";
			}
			$withstring = "with " . implode(", ", $withstrings) . " ";
		}
			
		// Build the query;
		$this->query = $withstring."SELECT $select FROM ".$this->table_with_schema().$joinstring.$wherestring.$groupstring.$unionstring.$orderstring.$limitstring;
				
		return $this->query;
	}
	
	// Given a selector code, e.g. a column name, 'count(*)', etc.,
	// preface it with the table name IF applicable
	// also return alias
	public function prefixSelectorWithTablenameIfApplicable($code, $alias="", $tablealias=""){
		if($tablealias == "") $tablealias = $this->get_table_alias();

        // Add table prefix if needed
		if(strpos($code,".") === false && strpos($code,"*") === false && strpos($code,"(") === false && strpos($code,"'") !== 0 && !is_numeric($code) && $code != "NULL"){
			if($alias == "") $alias = $code;
			$code = $tablealias . "." . $code;
		}
		
		return array($code, $alias);		
	}
	
	// Add a join with this base table
	// othertablefield and thistablefield may be single string field names, or parallel arrays containing multiple field names (they must have the same number of fields, in the order in which they should be associated)
	public function add_join(
			$othertablefield = "", 
			$othertablename = "", 
			$otherschema = "", 
			$thistablefield = "", 
			$thistablename = "", 
			$thisschema = "", 
			$direction = "append", 
			$jointype = "LEFT"
		)
		{
		if($thistablename == "") $thistablename = $this->table;
		if($thisschema == "") $thisschema = $this->schema;
		
		// Test for the fields variables that are needed
		if($othertablename == ""){
			throw new ErrorException("Join needs to join to a table.");
		}
		if($jointype != "CROSS" && ($othertablefield == "")){
			throw new ErrorException("Non-cross Join needs to join to a field in the other table.");
		}
		if($jointype != "CROSS" && ($thistablefield == "")){
			throw new ErrorException("Non-cross Join needs to join a field from this table.");
		}
		
		//if($otherschema == "") $otherschema = $this->schema; 
		// DO NOT add schema if it's blank. We might not have schema e.g. if it's a table generated in the query with 'with'
		
		$newjoin = new SQLJoin($othertablefield, $othertablename, $otherschema, $thistablefield, $thistablename, $thisschema, $jointype);
		
		if($newjoin->table2 == $this->table) $newjoin->table2alias = $this->get_table_alias();
		$newjoin->table1alias = $this->generate_join_alias($newjoin->table1);
		$this->joincount++;
		
		if($direction == "prepend") array_unshift($this->joins, $newjoin);
		else $this->joins[] = $newjoin;
	}
	
	// Returns true if this query has a join to the given table
	public function has_join($tablename){
		foreach($this->joins as $join){
			if($join->table1 == $tablename || $join->table2 == $tablename){
				return true;
				break;
			}
		}
		return false;
	}
	
	// Returns the number of joins this query has to the given table
	public function count_joins($tablename){
		$ct = 0;
		
		foreach($this->joins as $join){
			if($join->table1 == $tablename || $join->table2 == $tablename){
				$ct++;
				continue;
			}
		}
		
		return $ct;
	}
	
	// Remove joins to a particular table
	public function remove_joins($tablename){
		$rmv = array();
		$joins = array();
		
		foreach($this->joins as $idx=>$j){
			if($j->table1 == $tablename || $j->table2 == $tablename){
				// skip
			}
			else{
				$joins[] = $j;
			}
		}

		$this->joins = $joins;
	}
	
	// Add an array of field names for a given table to the select field list
	public function add_table_selectfields($tablealias, $fields, $aggregator="", $alias=false){
		foreach($fields as $field){
			 $selector = $tablealias.".".$field;
			 if($aggregator != "") $selector = $aggregator."(".$selector.")";
			 if($alias) $selector .= " ".$field;
			 $this->selectfields[] = $selector;
		}
		return $this->selectfields;
	}
	
	// Download csv
	public function download_csv($filename="export.csv", $tmpfolder="downloads", $headings = true){
		global $db;
		$csvFilename = false;
		
		$this->build_query();
		$csvFilename = $db->get_csv($this->query, $this->bind_variables, $filename, $tmpfolder, $headings);
		
		/* // Commented out becuase it prints even when a fallback is used and the CSV is created
		if(!$csvFilename){
			throw new ErrorException("CSV could not be created.");
		}
		*/
		
		return $csvFilename;
	}
	
	// Support
	private function createUnionString($uniquery, $idx = ""){
			
		$bindVariables = array();
		if(isset($this->unionBindVariables)){
			if($idx !== "") $bindVariables = $this->unionBindVariables[$idx];
			else $bindVariables = $this->unionBindVariables;
		}
		
		$unionstring = "";
		
		if($uniquery != ""){
			$unionstring = " UNION " . $uniquery;
			if(isset($bindVariables) && !empty($bindVariables)){
				$this->bind_variables = array_merge($this->bind_variables, $bindVariables);
			}
		}
		
		return $unionstring;
	}
	
	// Support	
	private function generate_join_alias($jointable=""){
		global $db;
		
		$sql_reserved_words = array();
		if(isset($db->sql_reserved_words) && !empty($db->sql_reserved_words)){
			$sql_reserved_words = $db->sql_reserved_words;
		}
		
		if($jointable=="" || in_array($jointable, $sql_reserved_words)){
			$alphabet = range('A', 'Z');
			$joinalias = $alphabet[$this->joincount];
		}
		else{
			
			$joinalias = $jointable;
			
			$numjoins = $this->count_joins($jointable);
			if($numjoins > 0) $joinalias .= $numjoins + 1; // i.e. if you join to dim_time twice, the first will be aliased to dim_time and the second will be aliased to dim_time2
		}
		
		if($joinalias == $this->get_table_alias()) $joinalias += '2'; // Differentiate if this is the same as the table alias
		
		return $joinalias;
	}

}

?>