#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()



def fetchAll(partitionquery, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(partitionquery)
        result = cur.fetchall()
        return result

def fetchOne(partitionquery, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(partitionquery)
        result = cur.fetchone()
        return result


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #preparing a select query string for range of ratings
    selectquery = '''
    SELECT '{4}{0}' , userid, movieid, rating
    FROM {1}{0}
    WHERE rating >= {2} AND rating <= {3}; 
    '''

    #preparing the partition  query string to fetch from rangeratingsmetadata
    partitionquery = '''
    SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>= {} and minrating<= {};
    '''.format(ratingMinValue, ratingMaxValue)

    result = []
    #getting the matched records for partition query and subsequent select query for a given max and min ratings
    for i, in fetchAll(partitionquery, openconnection):
        result += fetchAll(selectquery.format(i, 'rangeratingspart', ratingMinValue, ratingMaxValue, 'RangeRatingsPart'), openconnection)

    #fetching the matching records from all the partitions for a range between min and max rating value
    for i in xrange(fetchOne('SELECT partitionnum FROM roundrobinratingsmetadata;', openconnection)[0]):
        result += fetchAll(selectquery.format(i, 'roundrobinratingspart', ratingMinValue, ratingMaxValue, 'RoundRobinRatingsPart'), openconnection)

    writeToFile('RangeQueryOut.txt', result)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    #preparing a select query string for a particular rating value
    selectquery = '''
    SELECT '{3}{0}' , userid, movieid, rating
    FROM {1}{0}
    WHERE rating = {2}
    '''
    #preparing the partition  query string to fetch from rangeratingsmetadata for a particular rating value
    partitionquery = '''
    SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>= {} and minrating<= {};
    '''.format(ratingValue,ratingValue)# ratingMaxValue)

    result = []
    #getting the matched records for partition query and subsequent select query for a given rating value
    for i, in fetchAll(partitionquery, openconnection):
        result += fetchAll(selectquery.format(i, 'rangeratingspart', ratingValue, 'RangeRatingsPart'), openconnection)

    #fetching the matching records from all the partitions for a single rating value
    for i in xrange(fetchOne('SELECT partitionnum FROM roundrobinratingsmetadata;', openconnection)[0]):
        result += fetchAll(selectquery.format(i, 'roundrobinratingspart', ratingValue, 'RoundRobinRatingsPart'), openconnection)

    writeToFile('PointQueryOut.txt', result)
    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
