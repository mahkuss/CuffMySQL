<html>
	<head>
		<link type='text/css' rel='stylesheet' href='db_interface.css'/>
		<title>Database Interface Test</title>
	</head>

	<body>
	<div class="divFull">
		<div class="divTop">
			<h2 align="center">MySQL Interface Testing</h2>
		</div>

		<div class="divLeft" >
			<?php
			$db = new mysqli("localhost", "test", "thisisnotatest", "testdb");
			//echo $db->connect_errno;
			//echo "</br>";
			if($db->connect_errno > 0){
				die('Connection failed: ' . $db->connect_error);
			}
			//echo "connected";

			$result = $db->query( "SHOW TABLES" );
			//echo var_dump( $result );

			$result->data_seek(0);

			echo "<h3 class='leftHeader'>Available Tables</h3>";
			echo "<ul>";
			while ( $row = $result->fetch_assoc() ) {
				foreach( $row as $key=> $value ) {
					echo "<li>" . $value . "</li>"; 
				}
			}
			echo "</ul><br/>";
			?>

			<br/>

			<?php
			$default_query = 
			"SELECT gene_exp_diff.gene_id, gene_exp_diff.gene, gene_exp_diff.q_value, genes_read_group.tracking_id, genes_read_group.exp_condition, genes_read_group.replicate, genes_read_group.fpkm FROM gene_exp_diff, genes_read_group WHERE gene_exp_diff.gene_id = genes_read_group.tracking_id AND gene_exp_diff.gene = 'Gnai3';"
			?>

			<form name="testForm" action="mysqli_query_test.php" method="post">
				<p>
				<textarea name="user_query" rows="14" cols="30"><?php echo htmlspecialchars($default_query);?>
				</textarea>
				<input type="submit" value="Query Db"><input type="reset" value="Reset">
				</p>
			</form>
		</div>

		<?php
		echo "<div class='divRight'>";
		echo "<h3>Returned Data</h3>";

/* Debug code for when POST method was failing

		if( $_SERVER['REQUEST_METHOD'] == 'POST' ) {
			echo "Data posted.";
			echo "<br/>";
		}
		else {
			echo "No data submitted.";
			echo "<br/>";
			}

		if( isset($_POST['user_query']) ) {
			echo "Data received.";
			echo "<br/>";
		}
	
		else {
			echo "No data received.";
			echo "<br/>";
			}

*/

		$sql = $_POST['user_query'];
		if ( !$result = $db->query( $sql ) ) {
			echo "Query failed: " . $db->errno . " - " . $db->error;
		}

		else {
			echo "<table>";
			echo "<tr>";
			$colnames = $result->fetch_fields();
			foreach( $colnames as $col ) {
					echo "<th>" . $col->name . "</th>";
				}
			echo "</tr>";
	
			while ( $row = $result->fetch_assoc() ) {
				echo "<tr>";
				foreach( $row as $key=> $value ) {
					echo "<td>" . $value . "</td>"; 
					}
				echo "</tr>";
			}
			echo "</table>";
		}
		echo "</div>";
		?>
	</div>
	</body>
</html>
