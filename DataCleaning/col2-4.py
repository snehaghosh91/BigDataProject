import sys
from pyspark import SparkContext
from csv import reader
import re
import time

def checkTimeValidity(timestring):
    if timestring == "":
        return 'NULL'

    try:
        time.strptime(timestring, '%H:%M:%S')
        groups = re.search('(\d{1,2}):(\d{1,2}):(\d{1,2})', timestring)
        if groups:
            found1 = int(groups.group(1))
            found2 = int(groups.group(2))
            found3 = int(groups.group(3))
            if found1 >= 24 or found1 < 0 or found2 > 60 or found2 < 0 or found3 > 60 or found3 < 0:
                return 'INVALID'
            return 'VALID'
    except:
        return 'INVALID'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    header = lines.first()
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    fromTime = lines.map(lambda x : (x[0],x[2], checkTimeValidity(str(x[2]))))
    toTime = lines.map(lambda x: (x[0], x[4], checkTimeValidity(str(x[4]))))
    invalidFromTime = fromTime.filter(lambda x: x[2] == "INVALID" or x[2] == "NULL")
    validFromTime = fromTime.filter(lambda x: x[2] != "INVALID" and x[2] != "NULL")
    invalidFromTime.saveAsTextFile("invalidFromTime.out")
    validFromTime.saveAsTextFile("validFromTime.out")
    invalidToTime = toTime.filter(lambda x: x[2] == "INVALID" or x[2] == "NULL")
    validToTime = toTime.filter(lambda x: x[2] != "INVALID" and x[2] != "NULL")
    invalidToTime.saveAsTextFile("invalidToTime.out")
    validToTime.saveAsTextFile("validToTime.out")
    topInvalidValueFrom = invalidFromTime.map(lambda x:((x[1],x[2]),1)).reduceByKey(lambda a,b:a+b)
    topInvalidValueFrom.saveAsTextFile("topInvalidValuesFromTime.out")
    topInvalidValueTo = invalidToTime.map(lambda x:((x[1],x[2]),1)).reduceByKey(lambda a,b:a+b)
    topInvalidValueTo.saveAsTextFile("topInvalidValuesToTime.out")
    sc.stop()
