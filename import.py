import os
import os.path
import sys
import psycopg2
import argparse
import logging
import datetime
import time
from import_conf import * # config file

parser = argparse.ArgumentParser(description='Parse delimited text files and load them into PostgreSQL. The script can optionally create tables based on the files. If table exists, it will append the data.')
parser = argparse.ArgumentParser()
parser.add_argument("path", help="path of delimited files. Example: ~/directory_of_files/")
parser.add_argument("--create_tables", help="Drops table if it exists. Creates db table.", action="store_true")
parser.add_argument("--delete_data", help="Deletes data from table without dropping table.", action="store_true")
args = parser.parse_args()

log_filename = datetime.datetime.now().strftime('log_%m-%d-%Y_%H:%M:%S.log')
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

conn_string = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(conn_info['host'], conn_info['dbname'], conn_info['user'], conn_info['password']) 
logging.info("Connecting to server...")
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
logging.info("Connected to server")

path = args.path
					
files = filter(lambda x: ".TXT" in x or ".txt" in x, os.listdir(path))
print files
logging.info("Reading %s files: %s" % (str(len(files)),str(files)))

processed_domain = []

for filename in files:
	column_skip = set() # some columns are 0 len 
	column_names = [] # cleaned up column names

	domain = None	# see if we have a matching entry and set the domain if so
	
	for key, value in table_names.items():
		if key in filename.lower():
			domain = value
			break
	if domain is None:
		print "unknown filename: %s" % filename
		logging.info("unknown filename: %s" % filename)
		continue
	
	
	with open(path + '/' + filename) as f:
	# f = open(path + '/' + filename)
	# get the columns
		column_names = f.readline().strip().split(delimiter)
		print "\nOpening " + filename
		print column_names

		if domain not in processed_domain: # first time seeing this domain - create table and adapter node
			processed_domain.append(domain) # push domain into processed list
		
			if args.create_tables:
				drop_string = "drop table if exists " + domain + ";"
				logging.info("Dropping table "+domain)
				print drop_string
				create_string = "create table " + domain + " (id_ serial,\n " + " varchar(1024),\n".join(column_names) + " varchar(1024));" 
				logging.info("Creating table "+domain)
				print create_string
				cursor.execute(drop_string)
				cursor.execute(create_string)
				conn.commit()
			
			if args.delete_data:
				delete_string = "delete from " + domain + ";"
				logging.info("Deleting data from " + domain)
				print "Deleting data from " + domain
				cursor.execute(delete_string)
				conn.commit()

		# now insert the data
		logging.info("Inserting data into "+domain+" from "+str(filename))
		
		row_count = 0 # how many rows are inserted
		line_num = 1 # which line number its reading from source file.  starts at 1 including header.
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
				 
			for col_idx, col in enumerate(l.split(delimiter)): 
				if "_dt" in str(column_names[col_idx]).lower() and col != '':
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
		
		logging.info("READ %s rows from %s | INSERTED %s rows into %s | SKIPPED %s including header row" % (line_num, filename, row_count, domain, skip_count))

logging.info("Finished processing %s files." % str(len(files)))
logging.info("Success!")
