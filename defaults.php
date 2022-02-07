<?php

$root = realpath($_SERVER["DOCUMENT_ROOT"]);

// DB connect
require_once("/var/inc/dbconnect.php");
if($connection_params["type"] == "Oracle"){
	require_once("$root/shared/sql/class/EngineOracle.class.php");
	$db = new EngineOracle();
}
else if($connection_params["type"] == "Postgres"){
	require_once("$root/shared/sql/class/EnginePostgres.class.php");
	$db = new EnginePostgres();
}
else{
	require_once("$root/shared/sql/class/EngineSQLServer.class.php");
	$db = new EngineSQLServer();
}
if(isset($connection_params["version"])) $db->version = $connection_params["version"];
$conn = $db->connect($connection_params);

// SQL classes
require_once("$root/shared/sql/class/SQLQuery.class.php");
require_once("$root/shared/sql/class/SQLSelect.class.php");
require_once("$root/shared/sql/class/SQLInsert.class.php");
require_once("$root/shared/sql/class/SQLUpdate.class.php");
require_once("$root/shared/sql/class/SQLDelete.class.php");
require_once("$root/shared/sql/class/SQLCreate.class.php");

?>