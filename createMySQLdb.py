"""
Tools for creating MySQL databases and populating the necessary tables from lab data.
"""

import sys
import os.path
import MySQLdb as db

# create a parent wrapper class for collections of datafiles that need to be parsed as part of a project
# Cuffdata will then be a subclass that defines the appropriate methods for its particular
# collection of files
# This will allow this to be further subclassed in the future as new collections of experimental data
# arise

class Cuffdata:
	def ___init___(self, dirpath, cursor):
		self.dirpath = dirpath
		self.gene_exp_diff = os.path.join(dirpath, 'gene_exp.diff')
		self.isoform_exp_diff = os.path.join(dirpath, 'isoform_exp.diff')
		self.tss_group_exp_diff = os.path.join(dirpath, 'tss_group_exp.diff')
		self.css_exp_diff = os.path.join(dirpath, 'css_exp.diff')
		
		self.success = 0 # write class method to change this to 1 if and only if db creation succeeds

	def parse_tables(self, cursor):
		"""
		Cuffdata class method to override superclass parse_tables method
		"""
		parse_diff(self.gene_exp_diff, cursor, 'gene_exp_diff')
		parse_diff(self.isoform_exp_diff, cursor, 'isoform_exp_diff')
		parse_diff(self.tss_group_exp_diff, cursor, 'tss_group_exp_diff')
		parse_diff(self.cds_exp_diff, cursor, 'cds_exp_diff')

	def parse_diff(self, fp, cursor, table):
		"""
		Cuffdata class method to parse []_exp.diff files (e.g., gene_exp.diff)
		to a MySQL database table
		"""

		try:
			cursor.execute( "CREATE TABLE %s "\
				"( test_id CHAR(18), gene_id CHAR(18), gene VARCHAR(14), "\
				"chrom VARCHAR(10), start_coord INT UNSIGNED, end_coord INT UNSIGNED, "\
				"sample_1 VARCHAR(10), sample_2 VARCHAR(10), status VARCHAR(7), "\
				"value_1 DOUBLE, value_2 DOUBLE, log2 DOUBLE, "\
				"test_stat DOUBLE, p_value DOUBLE, q_value DOUBLE, significant VARCHAR(3) )" %(table) )

		except MySQLdb.Error, e:
				print("Error %d: %s" %(e.args[0], e.args[1]))

		with open(fp, 'r') as f:
			
			next(f) # skip column header line

			for line in f:

				# split line and retrieve values in separate variables
				test_id, gene_id, gene, locus, sample_1, sample_2, status, \
				value_1, value_2, log2, test_stat, p_value, q_value, significant \
				= line.split()

				'''locus is in format chrom:start-end (e.g., X:34534-36213)
					we need to split this into 3 separate values (chrom, start_coord,
					end_coord) to facilitate placement into a MySQL table'''
			# MOVE THIS BLOCK TO A CLASS METHOD
				try:
					chrom, coord = locus.split(':')
					start_coord, end_coord = coord.split('-')
				except:
					print(locus)

				# sometimes log2 is inf or -inf due to a 0 in one condition
				# MySQL does not support this notation so change to 
				# strings Float.MAX_VALUE and Float.MIN_VALUE, respectively
				if log2 == 'inf': 
					log2 = 'Float.MAX_VALUE'
				elif log2 == '-inf': 
					log2 = 'Float.MIN_VALUE'

				# insert row into MySQL table

				cmd = "INSERT INTO %s " \
				"( test_id, gene_id, gene, chrom, start_coord, end_coord, "\
				"sample_1, sample_2, status, value_1, value_2, log2, "\
				"test_stat, p_value, q_value, significant ) "\
				"VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' )"\
					%( table, test_id, gene_id, gene, chrom, start_coord, end_coord,\
						sample_1, sample_2, status, value_1, value_2, log2,\
						 test_stat, p_value, q_value, significant )
				try:
					cursor.execute(cmd)
				except MySQLdb.Error, e:
					print("Error %d: %s" %(e.args[0], e.args[1]))

		

	def parse_count_tracking(self, fp, cursor, table):
		"""
		Cuffdata class method to parse [].count_tracking files (e.g., genes.count_tracking)
		to a MySQL database table
		"""
		pass

	def parse_fpkm_tracking(self, fp, cursor, table):
		"""
		Cuffdata class method to parse [].fpkm_tracking files (e.g., genes.fpkm_tracking)
		to a MySQL database table
		"""

		# fpkm_tracking table consists of 8 fixed columns followed by a variable number of columns
		# depending on the number of samples in the analysis; we will create the base table
		# with the 8 fixed columns, then read in the header line of the file to insert the
		# remaining variable number of columns

		"""
		Note regarding the 'class_code' column from Cufflinks manual (http://cufflinks.cbcb.umd.edu/manual.html#class_codes):
		If you ran cuffcompare with the -r option, tracking rows will contain the following values. 
		If you did not use -r, the rows will all contain "-" in their class code column.
		"""
		# create base table using this data
		try:
			cursor.execute( "CREATE TABLE %s "\
				"( tracking_id CHAR(18), class_code CHAR(1), nearest_ref_id CHAR(18), "\
				"gene_id CHAR(18), gene_short_name VARCHAR(14), tss_id VARCHAR(60), "\
				"chrom VARCHAR(10), start_coord INT UNSIGNED, end_coord INT UNSIGNED, "\
				"length DOUBLE, coverage DOUBLE )" %( table ) )

		except MySQLdb.Error, e:
			print("Error %d: %s" %( e.args[0], e.args[1] ))


		# fpkm_tracking will have a variable number of columns depending on number of samples
		# therefore we now need to actually read in the header line of the file and parse it to
		# create the MySQL table

		with open(fp, 'r') as f:
			
			# read in the column header line and extract the sample columns (starts at column 9)
			sample_data = f.readline().split()[9:]
			
			
			# variable amount of sample data will extend from column 9 until the end
			sample_data = columns[ 9: ]
			# there are always 4 columns per sample so we can determine the number of samples
			num_samples = len( sample_data ) // 4
			
			# now need to iterate through sample_data and add table column for each entry
			# the first three columns will be FLOAT but the fourth is VARCHAR(7) which slightly
			# complicates the code

			for i in range(len( sample_data )):
				if ( i == 0 ) or ( (i + 1) % 4 != 0 ):
					try:
						cmd = "ALTER TABLE %s ADD %s FLOAT"  %( table, sample_data[i] )
						cursor.execute( cmd ) 

					except MySQLdb.Error, e:
						print("Error %d: %s" %( e.args[0], e.args[1] ))

				else:
					try:
						cmd = "ALTER TABLE %s ADD %s VARCHAR(7)"  %( table, sample_data[i] )
						cursor.execute( cmd )
					except MySQLdb.Error, e:
						print("Error %d: %s" %( e.args[0], e.args[1] ))

			"""
			To facilitate building a command to insert each row of data, extract
			the column names to a list. While this is not ideal for preventing 
			SQL injection, that is not really a concern here as this
			is just a tool to build the db and is not subject to user input.
			"""
			try:
				cmd = "SELECT `COLUMN_NAME` "\
					"FROM `INFORMATION_SCHEMA`.`COLUMNS` "\
					"WHERE `TABLE_SCHEMA`='testdb'"\
					"AND `TABLE_NAME`='%s'" %(table, )

				cursor.execute( cmd )

				column_names = cursor.fetchall()

			except MySQLdb.Error, e:
				print("Error %d: %s" %(e.args[0], e.args[1]))

			column_cmd = 'INSERT INTO %s ( ' %( table, ) + ', '.join( [ x[0] for x in column_names ] ) + ' )'

			# now loop through rows of file, building the VALUES part of MySQL command, and insert row into growing table
			
			for line in f:	# iterator should now be at line 2 of the file because header line read above

				# split line and retrieve values in separate variables
				row_data = line.split()

				# initialize accumulator to collect component strings of VALUE instruction
				# the first 6 items of each row need no additional processing so we can initialize
				# with these values

				row_cmd = [ item for item in row_data[ 0:6 ] ]

				# locus, length, and coverage values require additional processing so capture
				# to named variables
				locus, length, coverage = row_data[ 6:9 ]

				'''locus is in format chrom:start-end (e.g., X:34534-36213)
						we need to split this into 3 separate values (chrom, start_coord,
						end_coord) to facilitate placement into a MySQL table'''
				# MOVE THIS BLOCK TO A CLASS METHOD
				try:
					chrom, coord = locus.split(':')
					start_coord, end_coord = coord.split('-')

				except:
					print(locus)

				# add these items to the accumulator row_cmd
				row_cmd.append( chrom )
				row_cmd.append( start_coord )
				row_cmd.append( end_coord )

				# length and coverage are sometimes reported as '-' which is incompatible
				# with format of MySQL column (FLOAT) so check for this and convert to NULL as needed

				if length == '-': length = None
				if coverage == '-': coverage = None

				# now append length and coverage values to accumulator row_cmd
				row_cmd.append( length )
				row_cmd.append( coverage )

				# all the remaining values in the row correspond to the sample data so
				# we can simply concatenate these to the end of the accumulator row_cmd
				row_cmd += row_data[ 9: ]

				# build the actual MySQL VALUES string from row_cmd
				row_cmd = " VALUES ( '" + "', '".join( [ item for item in row_cmd ] ) + "' )"

				# join column_cmd and row_cmd to generate the actual MySQL insert statement for current row of data
				cmd = column_cmd + row_cmd

				# insert the row into table
				try:
					cursor.execute( cmd )

				except MySQLdb.Error, e:
					print( "Error: %d: %s" %( e.args[0], e.args[1] ))


	def parse_read_group_tracking(self, fp, cursor, table):
		"""
		Cuffdata class method to parse [].read_group_tracking files (e.g., genes.read_group_tracking)
		to a MySQL database table
		"""

		# first create the table: note that the 'condition' column is renamed 'exp_condition'
		# because 'condition' is a reserved word in MySQL
		try:
			cmd = "CREATE TABLE %s "\
				"( tracking_id CHAR(18), exp_condition VARCHAR(12), replicate INT, "\
				"raw_frags DOUBLE, internal_scaled_frags DOUBLE, external_scaled_frags DOUBLE, "\
				"fpkm DOUBLE, effective_length DOUBLE, status VARCHAR(6) )" %( table, )

			cursor.execute( cmd )

		except MySQLdb.Error, e:
				print("Error %d: %s" %( e.args[0], e.args[1] ))


		with open(fp, 'r') as f:
			
			next(f) # skip column header line

			for line in f:

				# split line and retrieve values in separate variables
				tracking_id, condition, replicate, raw_frags, internal_scaled_frags, \
				external_scaled_frags, fpkm, effective_length, status = line.split()		

				''' Effective_length might sometimes be represented by a hyphen (i.e., when
					Cuffdiff is run with the --no-effective-length-correction flag). To ensure
					compatability with MySQL we will check for this and convert to NULL as needed.
				'''	
				if effective_length == '-': effective_length = None

				# insert row into MySQL table

				cmd = "INSERT INTO %s "\
				"( tracking_id, exp_condition, replicate, raw_frags, internal_scaled_frags, "\
				"external_scaled_frags, fpkm, effective_length, status ) "\
				"VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' )"\
					%( table, tracking_id, condition, replicate, raw_frags, internal_scaled_frags,\
						external_scaled_frags, fpkm, effective_length, status )
				
				try:
					cursor.execute(cmd)
				
				except MySQLdb.Error, e:
					print("Error %d: %s" %(e.args[0], e.args[1]))


	def parse_run_info(self, fp, table):
		"Cuffdata class method to parse run.info file and record as metadata"
		pass


if __name__ == '__main__':

	EXPERIMENT_ID = ''
	DIRNAME = ''
	con = MySQLdb.connect(host='localhost', user='user', passwd='passwd')
	cursor = con.cursor()
	cursor.execute("CREATE DATABASE IF NOT EXISTS %s" %(EXPERIMENT_ID))
	cuff = Cuffdata(DIRNAME, cursor)
	if a.success:
		con.commit()
		print("Database successfully created")
	else:
		con.rollback()
		print("Error in database creation") # figure out way to make this more informative

	cursor.close()
	con.close()



"""
example query across tables:

SELECT gene_exp_diff.gene_id, gene_exp_diff.gene, gene_exp_diff.q_value,  
genes_read_group.tracking_id, genes_read_group.exp_condition, genes_read_group.replicate, genes_read_group.fpkm 
FROM gene_exp_diff, genes_read_group 
WHERE gene_exp_diff.gene_id = genes_read_group.tracking_id AND gene_exp_diff.gene = 'Gnai3';

"""