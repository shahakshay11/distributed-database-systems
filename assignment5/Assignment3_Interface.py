#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

def fetchOne(statement, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(statement)
        return cur.fetchone()


def fetchAll(statement, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall()

def executeStatement(statement, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(statement)
        openconnection.commit()

def getPartitionCount(prefix, openconnection):
    return fetchOne("SELECT count(*) FROM pg_stat_user_tables WHERE relname like '%s%%'" % prefix, openconnection)[0]

def create_like_table(TableName, LikeTableName, openconnection):
    statement = 'CREATE TABLE {} ( like {} including all)'.format(TableName, LikeTableName)
    executeStatement(statement, openconnection)

def sort_table(InputTable, SortingColumnName, PartitionTable, start, end, openconnection):
    #creating partition table
    create_like_table(PartitionTable, InputTable, openconnection)

    # insert into partition table
    sql_statement = 'INSERT INTO {} SELECT * FROM {} WHERE {} ORDER BY {}'
    if start is None:
        whereclause = '{0} <= {1}'.format(SortingColumnName, end)
    else:
        whereclause = '{0} > {1} AND {0} <= {2}'.format(SortingColumnName, start, end)
    sql_statement = sql_statement.format(PartitionTable, InputTable, whereclause, SortingColumnName)

    executeStatement(sql_statement, openconnection)

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.

    #Creating the range partitions
    no_threads = 5
    #rangePartition(InputTable, no_threads, openconnection)

    #Get the min max values of the sorting column of the given table
    sql_query = 'SELECT MIN({0}),MAX({0}) from {1}'.format(SortingColumnName,InputTable)

    min_val, max_val = fetchOne(sql_query,openconnection)

    #Getting the partition size
    partition_size = float(max_val - min_val) / no_threads

    partitions = [[min_val + (i - 1) * partition_size, min_val + i * partition_size] for i in range(1, no_threads + 1)]
    partitions[0][0] = None

    #preparing the function arguments including the partition table to passed to threads 
    func_args = []
    for i,(start,end) in enumerate(partitions):
        partition_table = '{}part{}'.format(InputTable,i)
        func_args.append(((InputTable, SortingColumnName, partition_table, start, end, openconnection)))

    #running 5 threads with corresponding fuctional arguments
    threads = [threading.Thread(target=sort_table, args=args) for args in func_args]
    list(map(lambda t: t.start(), threads))
    list(map(lambda t: t.join(), threads))

    #Creating the output table as same schema that of InputTable
    create_like_table(OutputTable,InputTable,openconnection)

    #inserting the sorted records from the thread result to output table
    statement = 'INSERT INTO {} Select * from {}'

    for i in range(len(partitions)):
        partition_table = '{}part{}'.format(InputTable, i)
        executeStatement(statement.format(OutputTable, partition_table), openconnection)



def create_partition_table(InputTable, col_name, min_col_val, numpartitions, partitionsize, openconnection):
    """
    Here we create the partition table based on min and max col value of the sorting column
    """
    for i in range(numpartitions):
        partition_table_name = '{}part{}'.format(InputTable, i)
        create_like_table(partition_table_name,InputTable,openconnection)

        sql_statement = 'INSERT INTO {} SELECT * FROM {} WHERE {}'
        if i == 0:
            where_clause = '{0}<= {1} \n'.format(col_name,min_col_val + partitionsize)
        else:
            end = min_col_val + (i + 1) * partitionsize
            start = end - partitionsize
            where_clause = '{0} > {1} and {0} <= {2}\n'.format(col_name,start,end)


        executeStatement(sql_statement.format(partition_table_name, InputTable,where_clause), openconnection)

def joinTable(input_table1, input_table2, Table1JoinColumn, Table2JoinColumn, partition_index, OutputTable, openconnection):
    """
    Creating the join construct for a given partition table
    """
    statement = 'INSERT INTO {} SELECT * FROM {}part{} JOIN {}part{} ON {} = {}'
    statement = statement.format(OutputTable, input_table1, partition_index, input_table2, partition_index, Table1JoinColumn, Table2JoinColumn)
    executeStatement(statement, openconnection)

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.

    numthreads = 5

    sql_query_table1 = 'SELECT MIN({0}),MAX({0}) from {1}'.format(Table1JoinColumn,InputTable1)
    sql_query_table2 = 'SELECT MIN({0}),MAX({0}) from {1}'.format(Table2JoinColumn,InputTable2)

    min_val1, max_val1 = fetchOne(sql_query_table1,openconnection)
    min_val2, max_val2 = fetchOne(sql_query_table2,openconnection)

    minval, maxval = min(min_val1, min_val2), max(max_val1, max_val2)
    partitionsize = float(maxval - minval) / numthreads

    #Creating partition tables for both the input tables with given join column
    create_partition_table(InputTable1, Table1JoinColumn, minval, numthreads, partitionsize, openconnection)
    create_partition_table(InputTable2, Table2JoinColumn, minval, numthreads, partitionsize, openconnection)


    #preparing the function arguments to passed to threads
    func_args = []
    for i in range(numthreads):
        func_args.append((InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, OutputTable, openconnection))

    #creating a empty join output table
    statement = 'CREATE TABLE {} AS (SELECT * FROM {} JOIN {} ON {} = {} LIMIT 0);'
    statement = statement.format(OutputTable, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn)
    executeStatement(statement, openconnection)

    #running 5 threads with corresponding fuctional arguments
    threads = [threading.Thread(target=joinTable, args=args) for args in func_args]
    list(map(lambda t: t.start(), threads))
    list(map(lambda t: t.join(), threads))


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
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
    con.commit()
    con.close()
