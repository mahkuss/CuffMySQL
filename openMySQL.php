<?php
	
	function connect_db($dbHost, $dbUser, $dbPass, $dbname) {
	
		global $db;

		if($db) {
			return $db;
		}

		$db = new mysqli($dbHost, $dbUser, $dbPass, $dbname);
		
		if($db->connect_errno > 0){
			die('Connection failed: ' . $db->connect_error);
		}

		else {
			return $db;
		}
	}	

	
?>