from __future__ import print_function

import sys
from operator import add
from decimal import Decimal

from pyspark import SparkContext, SparkConf


bucket = "capstonedata2"
filename = "airlines_ontime_summ2.csv"
s3link = "s3n://"+bucket+"/"+filename
s3filename = "s3n://"+bucket+"/"+filename

def carrier_tuple(line):
    try:
        return ((line.split(',')[2], 1), float(line.split(',')[12]))
    except:
        return ('error', 0)


if __name__ == "__main__":

    sc = SparkContext(appName="TopOntimeCarriers")
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
    .map(lambda (label, (value_sum, count)): (label[0], value_sum / count))\
    .map(lambda (x,y): (y,x))\
    .sortByKey()

    topcounts=averageByKey.zipWithIndex().filter(lambda x: x[1] < 10).map(lambda (x,y): (x[1],x[0])).coalesce(1)
    topcounts.saveAsTextFile("s3n://"+bucket+"/topontimecarriers-output")

    output = topcounts.collect()
    for carrier,delay in output:
        print("%s: %f" % (carrier, delay))
    sc.stop()
