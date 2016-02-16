from __future__ import print_function


import sys
from cassandra.cluster import Cluster

from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta

bucket = "capstonedata2"
filename = "airlines_ontime_summ2.csv"
s3link = "s3n://"+bucket+"/"+filename
cluster = Cluster(
    contact_points=['54.86.121.193'],
)
keyspace = 'aviation'
table = 'spark_q32'
session = cluster.connect(keyspace)
#origin, connectionport, destination, odate, ddate, delay, dtime, otime
prepared_stmt = session.prepare ( "INSERT INTO " + keyspace + "." + table +
                                  " (origin, connectionport, destination, odate, ddate, delay, dtime, otime) "
                                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

def carrier_tuple(line):
    try:
        airport = line.split(',')[4]
        destination = line.split(',')[6]
        carrier = line.split(',')[2]
        depdate = line.split(',')[0]
        deptime = line.split(',')[8]
        delay = line.split(',')[12]
        dephour = int(deptime[1:3]);
        if dephour >= 12:
            newdepdate = datetime.strptime(depdate, "%Y-%m-%d") - timedelta(days=2)
            newdepdate_str=newdepdate.strftime("%Y-%m-%d")
            return ((airport, newdepdate_str), (destination, depdate, deptime, float(delay), 2))
        else:
            return ((destination, depdate), (airport, depdate, deptime, float(delay), 1))
    except:
        return ('error', 0)


if __name__ == "__main__":

    sc = SparkContext(appName="Sightseens")
    if len(sys.argv) != 2:
        textFile = sc.textFile(s3link)
    else:
        textFile = sc.textFile(sys.argv[1], 1)
    data = textFile.map(lambda line: carrier_tuple(line))\
    .filter(lambda line: line[0] <> 'error')
    print('------------INPUT DATA PROCESSED-------------')
    leg1 = data\
    .filter(lambda value: value[1][4]==1)
#    .map(lambda v: ((v[0][0], v[0][1], v[1][0]),(v[1][3], v[1][1], v[1][2], v[1][4])))
#    .reduceByKey(min)\
#    .map(lambda v: ((v[0][0], v[0][1]), (v[0][2], v[1][1], v[1][2], v[1][0], v[1][3])))

    print('------------LEG1 BUILDED-------------')
    leg2 = data.filter(lambda value: value[1][4]==2)\
    .join(leg1)
#    .map(lambda v: ((v[0][0], v[0][1], v[1][0]),(v[1][3], v[1][1], v[1][2], v[1][4])))\
    #    .reduceByKey(min)\
#    .map(lambda v: ((v[0][0], v[0][1]), (v[0][2], v[1][1], v[1][2], v[1][0], v[1][3])))\
    print('------------LEG2 BUILDED-------------')
#    result = leg1.join(leg2)
    print('------------RESULTS BUILDED-------------')
    output=leg2.collectAsMap()

    for airports in output.keys():
#        print(airports)
#        print(output[airports])
        route = {}
        route['connectionport'] = airports[0]
        for val in output[airports]:
            if val[4] == 1:
                route['airport'] = val[0]
                route['depdate'] = val[1]
                route['deptime'] = val[2]
                route['depdelay'] = str(val[3])
            elif val[4] == 2:
                route['depconndate'] = val[1]
                route['depconntime'] = val[2]
                route['depconndelay'] = str(val[3])
                route['finaldestination'] = val[0]
#        print(route)
            #origin, connectionport, destination, odate, ddate, delay, dtime, otime
        bound_stmt = prepared_stmt.bind([route['airport'], route['connectionport'], route['finaldestination'],
                                         route['depdate'], route['depconndate'], route['depconndelay'],
                                         route['depconntime'], route['deptime']])
        stmt = session.execute(bound_stmt)
    sc.stop()
