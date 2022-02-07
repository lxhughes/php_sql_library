A database engine-agnostic way to build common SQL queries in PHP, whether you are using SQL Server, Postgres, or Oracle. 

SQL queries must be written differently depending on the database engine they use. Additionally, duilt-in PHP database connect & execute functions differ depending on your database engine. Because this SQL library can build queries for, and connect to, three different engines, you can move the same code to a different site using a different engine, or switch to a different DB engine on the same site, without rewriting all your PHP code. 

To use:
1. Create a /var/inc/dbconnect.php page (or put it somewhere else and edit defaults.php to refer to the correct location; avoid putting it in /var/public/html and version-controlling it in Github, as it contains the password to the database.) dbconnect.php defines the $connection_params hash, which defines DB engine and version you are using on this site.

$connection_params = array(
    "type" => "Oracle",
    "version" => 18.2,
    "username" => "database_user",
    "password" => "database_password",
    "admin_email" => "me@mywebsite.com"
);

2. Include defaults.php on your various PHP code pages where you'd like to use the SQL query library.

require_once("defaults.php")

3. Use the SQL query library to build your queries instead of manually writing queries and executing them with built in PHP functions.

Example:

$select = new SQLSelect("my_table", "my_schema");
$select->selectfields = array("col1", "col2", "col3" ...) // Optionally define fields; defaults to *
// Optionally define other parameters such as orderfields, groupfields, etc.
$result = $select->execute();

There are also classes for other types of SQL queries (create, insert, update, delete).

Depending on the DB defined in dbconnect.php, SQL will output a slightly different query and use different PHP functions to connect to the database. 