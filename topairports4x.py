from __future__ import print_function

import sys
from operator import add, itemgetter
from decimal import Decimal
from cassandra.cluster import Cluster

from pyspark import SparkContext, SparkConf


bucket = "capstonedata2"
filename = "airlines_ontime_summ2.csv"
s3link = "s3n://"+bucket+"/"+filename
cluster = Cluster(
    contact_points=['54.86.121.193'],
)
keyspace = 'aviation'
table = 'spark_q22'
session = cluster.connect(keyspace)
prepared_stmt = session.prepare ( "INSERT INTO " + keyspace + "." + table +
                                  " (airport, destination, avgdelay) VALUES (?, ?, ?)")

def carrier_tuple(line):
    try:
        return ((line.split(',')[4], line.split(',')[6], 1), float(line.split(',')[9]))
    except:
        return ('error', 0)


if __name__ == "__main__":

    sc = SparkContext(appName="Top10Airports4X")
    if len(sys.argv) != 2:
        textFile = sc.textFile(s3link)
    else:
        textFile = sc.textFile(sys.argv[1], 1)
    data = textFile.map(lambda line: carrier_tuple(line))\
    .filter(lambda line: line[0] <> 'error')

    sumCount = data.combineByKey(lambda value: (value, 1),
        lambda x, value: (x[0] + value, x[1] + 1),
        lambda x, y: (x[0] + y[0], x[1] + y[1]))

    averageByKey = sumCount\
    .map(lambda (label, (value_sum, count)): (label[0], (label[1] , value_sum / count)))\
    .groupByKey()\
    .map(lambda x : (x[0], list(x[1])))

    airportsStat = averageByKey.collectAsMap()
    for airport in airportsStat.keys():
#        print(airport)
        sorted_dest = sorted(airportsStat[airport], key=lambda x:x[1])
        for pair in sorted_dest[:10]:
            bound_stmt = prepared_stmt.bind([airport, pair[0], pair[1]])
            stmt = session.execute(bound_stmt)
#            print(pair)
    sc.stop()
