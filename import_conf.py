# Filename <=> Table mapping
# filename_substr : table name
table_names = {	
				'filename1' : 'TABLE1',
				'filename2' : 'TABLE2',
				'filename3' : 'TABLE3'
}

conn_info = {
				'host' : 'localhost',
				'dbname' : 'mydb',
				'user' : 'postgres',
				'password' : 'postgres'
}

delimiter = '$'

# If the file has date field (ending with '_dt'), it will be converted into Postgres friendly date (ex: 2012-12-01)
file_date_format = "%Y%m%d"

# Sometimes there is an extra delimiter at the end of each line. This optionally removes the last character of each line. 
remove_last_char = True
