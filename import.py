import os
import os.path
import re
import sys
import psycopg2
import argparse
import logging
import datetime
import time
import xlrd
import csv
from import_conf import * # config file

def process_txt_files(f, column_names):
	line_num = 1 # which line number its reading from source file.  starts at 1 including header.
	row_count = 0 # how many rows are inserted
	skip_count = 1 # how many rows are skipped. starts at 1 including header.
	
	for line in f:
		vals = []
		line_num = line_num + 1
			
		# line.strip()[:-1] is there to remove an extra '$' (delimiter) at the end of line. 
		# This is only used if you have one extra delimiter at the end of each line.
		if remove_last_char:
			l = line.strip()[:-1]
		else:
			l = line.strip()
				 
		if file_type == "txt":
			col = l.split(txt_delimiter)
		elif file_type == "csv":
			col = csv.reader([l], skipinitialspace=True)
			col = col.next()
			
		for col_idx, col in enumerate(col): 
			if "-date" in str(column_names[col_idx]).lower() and col != '':
				try:
					# convert date format to Postgres friendly date
					t = datetime.datetime.strptime(col, file_date_format) 
					vals.append(t.strftime("%Y-%m-%d"))
				except ValueError as e:
					skip_count = skip_count + 1
					logging.error("ERROR converting date on line {0} of {1}\nData: {2}\n{3}".format(row_count, filename, line, e))
					vals = [] # don't insert this line
					break
			else:
				vals.append(col)
			
		# sometimes a line is formatted incorrectly
		# skip row and rollback transaction
		if vals:
			try:
				insert_string = "insert into " + domain + " ("+",".join(column_names) + ")" + " values ("  + ",".join(['%s'] * len(vals)) + ")"
				cursor.execute(insert_string, vals)
				conn.commit()
				row_count=row_count+1
			except psycopg2.Error as e:
				conn.rollback()
				skip_count = skip_count + 1
				logging.error("ERROR inserting to DB on line {1} of {2}.\nData: {3}\n{0}".format(e.pgerror, row_count, filename, line))
	
	return line_num,row_count

def process_xls_files(sheet):
	line_num = 1 # which line number its reading from source file.  starts at 1 including header.
	row_count = 0 # how many rows are inserted
	skip_count = 1 # how many rows are skipped. starts at 1 including header.
	
	for row_idx in xrange(1, sheet.nrows):
		vals = []
		line_num = line_num + 1
		
		for col_idx, col in enumerate(sheet.row(row_idx)):
			if col_idx not in column_skip:
				if isinstance(col.value, str) or isinstance(col.value, unicode):
					if isinstance(col.value, str) and len(col.value.strip()) == 0:
						vals.append(None)
					elif isinstance(col.value, unicode) and len((col.value).strip()) == 0:
						vals.append(None)
					else:
						vals.append(col.value[:1023])
				elif "date" in str(column_names[col_idx]).lower() or "dtc" in str(column_names[col_idx]).lower(): # converts excel-stored float into dates
					if (col.value is not None) and int(col.value) > 60:
						vals.append(str(datetime.datetime(*xlrd.xldate_as_tuple(col.value, sheet.book.datemode))))
					else:
						vals.append(None)
				else:
					vals.append(col.value)
					
		# values = ['%s'] * len(vals)
		# values = [value.replace('\'TIMESTAMP', 'TIMESTAMP\'') for value in values]

		insert_string = "insert into " + domain + " ("+",".join(column_names) + ")" + " values ("  + ",".join(['%s'] * len(vals)) + ")"
		#print insert_string
		row_count=row_count+1
		cursor.execute(insert_string, vals)
		conn.commit()
	
	return line_num,row_count

def create_tables(domain, column_names):
	drop_string = "drop table if exists " + domain + ";"
	logging.info("Dropping table "+domain)
	print drop_string
	create_string = "create table " + domain + " (id_ serial,\n " + " varchar(1024),\n".join(column_names) + " varchar(1024));" 
	logging.info("Creating table "+domain)
	print create_string
	cursor.execute(drop_string)
	cursor.execute(create_string)
	conn.commit()

def delete_data(domain):
	delete_string = "delete from " + domain + ";"
	logging.info("Deleting data from " + domain)
	print "Deleting data from " + domain
	cursor.execute(delete_string)
	conn.commit()

########## Main Starts Here ############
parser = argparse.ArgumentParser(description='Parse delimited text files and load them into PostgreSQL. The script can optionally create tables based on the files. If table exists, it will append the data.')
parser.add_argument("path", help="path of excel or text files. Example: ~/directory_of_files/")
parser.add_argument("--file", help="only process a SINGLE file from [path] argument and ignore other files. Example: --file filename.xls will process only ~directory_of_files/filename.xls")
parser.add_argument("--create_tables", help="Drops table if it exists. Creates db table.", action="store_true")
parser.add_argument("--delete_data", help="Deletes data from table without dropping table.", action="store_true")
args = parser.parse_args()

path = args.path
files = None
file_type = None

if args.file:
	file = args.file
	files = filter(lambda x: file == x, os.listdir(path))
	if not files:
		print "Can't find %s. Bye bye." % file
		sys.exit()
		
	if ".txt" in file.lower():
		file_type = "txt"
	elif ".csv" in file.lower():
		file_type = "csv"
	elif ".xlsx" in file or ".xls" in file:
		file_type = "xls"
	else: 
		print "File is not .txt or .xls / .xlsx. Please specify a new file."
		sys.exit()
else: # process all files in [path]
	files = filter(lambda x: ".txt" in x.lower(), os.listdir(path))
	file_type = "txt"
	if not files:
		files = filter(lambda x: ".csv" in x, os.listdir(path))
		file_type = "csv"
		if not files:
			files = filter(lambda x: ".xlsx" in x or ".xls" in x, os.listdir(path))
			file_type = "xls"
			if not files:
				print "No files are found."
				sys.exit()

print files

log_filename = datetime.datetime.now().strftime('log_%m-%d-%Y.log')
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

conn_string = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(conn_info['host'], conn_info['dbname'], conn_info['user'], conn_info['password']) 
logging.info("Connecting to server...")
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
logging.info("Connected to server")

logging.info("Reading %s files: %s" % (str(len(files)),str(files)))
processed_domain = [] # initialize domain <-> filename mapping

for filename in files:
	column_skip = set() # some columns are 0 len 
	column_names = [] # cleaned up column names
	xls_column_names = [] # original column names (from excel)
	lines_read = 0
	rows_inserted = 0

	domain = os.path.splitext(filename)[0].replace(' ','').lower() # create table based on filename
	
	if domain is None:
		print "File does not match domain: %s" % filename
		logging.info("File does not match domain: %s" % filename)
		continue
	
	
	# Get columns
	if file_type == "txt" or file_type == "csv":
		with open(path + '/' + filename, 'rU') as f:
			# get the columns by reading first line
			if file_type == "csv":
				column_names = f.readline().strip().split(',')
			else:
				column_names = f.readline().strip().split(txt_delimiter)

			# get rid of unicodes, dashes and spaces in column names
			column_names = [re.sub(r'[\W]+','',c.replace('\xef\xbb\xbf', '').replace('-','_').replace(' ','_')) for c in column_names]
		
			print "\nGetting columns from " + filename
			print column_names
			
			# Create table from columns
			if domain not in processed_domain: # first time seeing this domain - create table and adapter node
				processed_domain.append(domain) # push domain into processed list
		
				if args.create_tables:
					create_tables(domain, column_names)
			
				if args.delete_data:
					delete_data(domain)
			
			# now insert the data
			logging.info("Inserting data into "+domain+" from "+str(filename))
			lines_read,rows_inserted = process_txt_files(f, column_names)

	elif file_type == "xls":
		sheet = xlrd.open_workbook(path + '/' + filename).sheets()[0]
		
		# get the columns
		for column_index, column in enumerate([col.value for col in sheet.row(0)]):
			# some of these end up being 0 len
			if(len(column) == 0):
				column_skip.add(column_index)
				continue
			# elif "comment" in str(column).lower(): # ignore comments
			# 	column_skip.add(column_index)
			# 	continue
			xls_column_names.append(str(column))
			column_names = [re.sub(r'[\W]+','',c.replace('-','_').replace(' ','_')) for c in xls_column_names]

		print "\nGetting columns from " + filename
		print column_names
		
		# Create table from columns
		if domain not in processed_domain: # first time seeing this domain - create table and adapter node
			processed_domain.append(domain) # push domain into processed list
		
			if args.create_tables:
				create_tables(domain, column_names)
			
			if args.delete_data:
				delete_data(domain)
		
		# now insert the data
		logging.info("Inserting data into "+domain+" from "+str(filename))
		lines_read,rows_inserted = process_xls_files(sheet)
	
	logging.info("----- READ %s rows (incl. header) from %s | INSERTED %s rows into %s -----" % (lines_read, filename, rows_inserted, domain))


logging.info("Finished processing %s files. All done!" % str(len(files)))
