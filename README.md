db-importer
===========

Python script that imports any Excel or text file with any delimiter into PostgreSQL database.

**Features:**

* Configuration file to set db connection info
* Reads any XLS or TXT file with any delimiter set in the config file
* Reads and parses multiple files into same or different tables
* Error logging and exception handling that returns skipped rows
* Option to create tables from filenames
* Option to append or delete data
* Option to convert date data from any format to SQL friendly format

**Usage:**

Insert or append data to existing tables:
```
python import.py [path/to/your/files]
```

Create tables (note: this will drop tables if they exist):
```
python import.py --create_tables [path/to/your/files]
```

Refresh data (delete data from tables and reload)
```
python import.py --delete_data [path/to/your/files]
```
	
**Configuration:**
```python
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
```

**Future Improvements**
* Create table columns based on XLS cell types.  Currently the script creates a table with all varchar columns.
* Insert into existing table that is not varchar.  Currently the script only inserts strings into columns.