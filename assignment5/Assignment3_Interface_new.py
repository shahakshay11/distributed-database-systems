#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading


def executeStatement(statement, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(statement)
        openconnection.commit()


def fetchOne(statement, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(statement)
        return cur.fetchone()


def fetchAll(statement, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall()


def getPartitionCount(prefix, openconnection):
    return fetchOne("SELECT count(*) FROM pg_stat_user_tables WHERE relname like '%s%%'" % prefix, openconnection)[0]


def deletePartitionTables(prefix, openconnection):
    partitionCount = getPartitionCount(prefix, openconnection)
    for i in range(partitionCount):
        executeStatement('DROP TABLE {}{}'.format(prefix, i), openconnection)


def createTableLike(TableName, LikeTableName, openconnection):
    statement = 'CREATE TABLE {} ( like {} including all)'.format(TableName, LikeTableName)
    executeStatement(statement, openconnection)


def sortTable(InputTable, SortingColumnName, PartitionTableName, start, end, openconnection):
    # create partition table
    createTableLike(PartitionTableName, InputTable, openconnection)

    # insert into partition table
    statement = 'INSERT INTO {} SELECT * FROM {} WHERE {} ORDER BY {}'
    if start is None:
        whereclause = '{0} <= {1}'.format(SortingColumnName, end)
    else:
        whereclause = '{0} > {1} AND {0} <= {2}'.format(SortingColumnName, start, end)
    statement = statement.format(PartitionTableName, InputTable, whereclause, SortingColumnName)

    executeStatement(statement, openconnection)


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    # Implement ParallelSort Here.
    numthreads = 5

    # partitions
    query = 'SELECT MIN({0}), MAX({0}) FROM {1}'.format(SortingColumnName, InputTable)
    minval, maxval = fetchOne(query, openconnection)
    partitionsize = float(maxval - minval) / numthreads
    partitions = [[minval + (i - 1) * partitionsize, minval + i * partitionsize] for i in range(1, numthreads + 1)]
    partitions[0][0] = None

    argslist = []
    for i, (start, end) in enumerate(partitions):
        PartitionTable = '{}part{}'.format(InputTable, i)
        argslist.append((InputTable, SortingColumnName, PartitionTable, start, end, openconnection))

    threads = [threading.Thread(target=sortTable, args=args) for args in argslist]
    list(map(lambda t: t.start(), threads))
    list(map(lambda t: t.join(), threads))

    # create output table
    createTableLike(OutputTable, InputTable, openconnection)

    # insert into output table
    statement = 'INSERT INTO {} SELECT * FROM {}'
    for i in range(len(partitions)):
        PartitionTable = '{}part{}'.format(InputTable, i)
        executeStatement(statement.format(OutputTable, PartitionTable), openconnection)

    return


def partitionTable(InputTable, ColumnName, mincolval, numpartitions, partitionsize, openconnection):
    # insert into partition table
    for i in range(numpartitions):
        PartitionTableName = '{}part{}'.format(InputTable, i)

        createTableLike(PartitionTableName, InputTable, openconnection)

        statement = 'INSERT INTO {} SELECT * FROM {} WHERE {}'
        if not i:
            whereclause = '{0} <= {1}\n'.format(ColumnName, mincolval + partitionsize)
        else:
            end = mincolval + (i + 1) * partitionsize
            start = end - partitionsize
            whereclause = '{0} > {1} AND {0} <= {2}\n'.format(ColumnName, start, end)

        executeStatement(statement.format(PartitionTableName, InputTable, whereclause), openconnection)


def joinTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, OutputTable, openconnection):
    statement = 'INSERT INTO {} SELECT * FROM {}part{} JOIN {}part{} ON {} = {}'
    statement = statement.format(OutputTable, InputTable1, i, InputTable2, i, Table1JoinColumn, Table2JoinColumn)
    executeStatement(statement, openconnection)


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Implement ParallelJoin Here.
    numthreads = 5

    # partitions
    query = 'SELECT MIN({0}), MAX({0}) FROM {1}'.format(Table1JoinColumn, InputTable1)
    minval1, maxval1 = fetchOne(query, openconnection)

    query = 'SELECT MIN({0}), MAX({0}) FROM {1}'.format(Table2JoinColumn, InputTable2)
    minval2, maxval2 = fetchOne(query, openconnection)

    minval, maxval = min(minval1, minval2), max(maxval1, maxval2)
    partitionsize = float(maxval - minval) / numthreads

    partitionTable(InputTable1, Table1JoinColumn, minval, numthreads, partitionsize, openconnection)
    partitionTable(InputTable2, Table2JoinColumn, minval, numthreads, partitionsize, openconnection)

    argslist = []
    for i in range(numthreads):
        argslist.append((InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, OutputTable, openconnection))

    statement = 'CREATE TABLE {} AS (SELECT * FROM {} JOIN {} ON {} = {} LIMIT 0);'
    statement = statement.format(OutputTable, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn)
    executeStatement(statement, openconnection)

    threads = [threading.Thread(target=joinTable, args=args) for args in argslist]
    list(map(lambda t: t.start(), threads))
    list(map(lambda t: t.join(), threads))

    return


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
