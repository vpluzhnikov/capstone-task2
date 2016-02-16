from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext, SparkConf


bucket = "capstonedata2"
filename = "airlines_ontime_summ3.csv"
s3link = "s3n://"+bucket+"/"+filename
s3filename = "s3n://"+bucket+"/"+filename

def airports_list(line):
    if line:
        return [line.split(',')[4], line.split(',')[6]]

if __name__ == "__main__":

    sc = SparkContext(appName="TopAirports")
    if len(sys.argv) != 2:
        textFile = sc.textFile(s3link)

    else:
        textFile = sc.textFile(sys.argv[1], 1)
    counts = textFile.flatMap(lambda line: (airports_list(line)))\
    .map(lambda line: (line, 1))\
    .reduceByKey(add)\
    .map(lambda (x,y): (y,x))\
    .sortByKey(False)

    topcounts=counts.zipWithIndex().filter(lambda x: x[1] < 10).map(lambda (x,y): (x[1],x[0])).coalesce(1)
    topcounts.saveAsTextFile("s3n://"+bucket+"/topairport-output")

    output = topcounts.collect()

    for (airport, count) in output:
        print("%s: %i" % (airport, count))
    sc.stop()

