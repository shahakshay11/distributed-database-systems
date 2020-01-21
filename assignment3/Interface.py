#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import math

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
	return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
	with openconnection.cursor() as cur,open(ratingsfilepath) as f:
		#creating the table ratings
		cur.execute('Create table ratings(\
			userid integer not null,\
			dummy1 varchar,\
			movieid integer not null,\
			dummy2 varchar,\
			rating numeric,\
			dummy3 varchar,\
			timestamp bigint not null default (extract(epoch from now()) * 1000) \
		)')

		# inserting the data into ratings using postgres command COPY_FROM
		cur.copy_from(f,'ratings',columns = ('userid','dummy1','movieid','dummy2','rating','dummy3','timestamp'),sep=":")

		#drop the redundant columns ( dummy1,dummy2,dummy3,timestamp)
		cur.execute(' alter table ratings drop column dummy1,drop column dummy2,drop column dummy3,drop column timestamp')
	pass

def executeStatement(statement, openconnection):
	#Executing the query statement and commiting to the DB
	with openconnection.cursor() as cur:
		cur.execute(statement)
		openconnection.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
	# Create the tables with the name "partition_no" based on number of partitions

	# create range partitions
	partitionsize = 5.0 / numberofpartitions

	#initializing the range query with respect to partition size
	rangequeries = ['0 <= rating AND rating <= %s' % partitionsize]

	#iterating over the numberofpartitions and updating the range queries accordingly
	for i in range(1, numberofpartitions - 1):
		rangequeries.append('%s <  rating AND rating <= %s' % (partitionsize * i, partitionsize * (i + 1)))

	#Adding the end range til 5( max value of rating is 5)
	if numberofpartitions > 1:
		rangequeries.append('%s < rating AND rating <= 5.0' % (partitionsize * (numberofpartitions - 1)))

	create = 'CREATE TABLE range_part%s (LIKE %s INCLUDING ALL) ;'
	insert = 'INSERT INTO range_part%s SELECT * FROM %s WHERE %s;'
	for partitionnum, rangequery in zip(xrange(numberofpartitions), rangequeries):
		#creating the partition table
		executeStatement(create % (partitionnum, ratingstablename), openconnection)
		#inserting the records from ratings to corresponding range partitions
		executeStatement(insert % (partitionnum, ratingstablename, rangequery), openconnection)


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
	cur = openconnection.cursor()
	RROBIN_TABLE_PREFIX = 'rrobin_part'
	'''
	pull out one record from the table and add to partition tables sequentially till all the records are not filled
	'''
	for i in range(numberofpartitions):
		partition_table_name = RROBIN_TABLE_PREFIX + str(i)
		cur.execute('Create Table %s (userid integer, movieid integer,rating numeric)'%(partition_table_name))
		cur.execute('Insert into %s (userid, movieid, rating) select userid, movieid, \
			rating from (select userid, movieid, rating, ROW_NUMBER() over() as rownum from %s) as temp where mod(temp.rownum-1, 5) = %s'%(partition_table_name,ratingstablename,str(i)))

	cur.close()
	openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
	#insert into ratings table
	con = openconnection
	cur = con.cursor()
	RROBIN_TABLE_PREFIX = 'rrobin_part'
	cur.execute('Insert into %s (userid, movieid, rating) values (%s,%s,%s)' %(ratingstablename,userid,itemid,rating))
	
	#Get the number of rows in the ratings table
	cur.execute("select count(*) from %s"%ratingstablename)
	num_rows = cur.fetchall()[0][0]

	#Get the number of partitions in the db starting with rrobin_part
	no_partitions = int(count_partitions(RROBIN_TABLE_PREFIX,openconnection))
	#Get the index of partition for which new entry is to be inserted
	index = (num_rows - 1) % no_partitions
	#insert to the respective partition index
	cur.execute('Insert into %s (userid, movieid, rating) values (%s,%s,%s)' %(RROBIN_TABLE_PREFIX + str(index),userid,itemid,rating))
	cur.close()
	con.commit()
	pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	RANGE_TABLE_PREFIX = 'range_part'
	#getting the number of range partitions
	numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)

	partitionnum = math.ceil(rating * numberofpartitions / 5.0)
	if partitionnum:
		partitionnum -= 1

	columns = ('userid', 'movieid', 'rating')
	#inserting the record in the ratings table
	insertOne(ratingstablename, (userid, itemid, rating), columns, openconnection)
	#inserting the record in the partition with index partition num
	insertOne('%s%s' % (RANGE_TABLE_PREFIX, partitionnum), (userid, itemid, rating), columns, openconnection)


def insertOne(table, row, columns, openconnection):
	# inserting the record to the given schema using the connection
	with openconnection.cursor() as cur:
		query = '''INSERT INTO %s (%s) VALUES (%s)''' % (
			table, ','.join(columns), ','.join(['%s'] * len(columns)))
		executeStatement(query % row, openconnection)

def createDB(dbname='dds_assignment'):
	"""
	We create a DB by connecting to the default user and database of Postgres
	The function first checks if an existing database exists for a given name, else creates it.
	:return:None
	"""
	# Connect to the default database
	con = getOpenConnection(dbname='postgres')
	con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	cur = con.cursor()

	# Check if an existing database with the same name exists
	cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
	count = cur.fetchone()[0]
	if count == 0:
		cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
	else:
		print 'A database named {0} already exists'.format(dbname)

	# Clean up
	cur.close()
	con.close()

def deletepartitionsandexit(openconnection):
	cur = openconnection.cursor()
	cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	l = []
	for row in cur:
		l.append(row[0])
	for tablename in l:
		cur.execute("drop table if exists {0} CASCADE".format(tablename))

	cur.close()

def deleteTables(ratingstablename, openconnection):
	try:
		cursor = openconnection.cursor()
		if ratingstablename.upper() == 'ALL':
			cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
			tables = cursor.fetchall()
			for table_name in tables:
				cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
		else:
			cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
		openconnection.commit()
	except psycopg2.DatabaseError, e:
		if openconnection:
			openconnection.rollback()
		print 'Error %s' % e
	except IOError, e:
		if openconnection:
			openconnection.rollback()
		print 'Error %s' % e
	finally:
		if cursor:
			cursor.close()

def count_partitions(prefix,openconnection):
	cur = openconnection.cursor()
	cur.execute("SELECT count(*) FROM pg_stat_user_tables WHERE relname like '%s%%'"%prefix)
	count = cur.fetchone()[0]
	cur.close()
	return count
