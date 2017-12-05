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

def returnCorrectedTime(timestring):
    if not timestring.strip():
        return timestring
    try:
        groups = re.search('(\d{1,2}):(\d{1,2}):(\d{1,2})', timestring)
        if groups:
            found1 = int(groups.group(1))
            found2 = int(groups.group(2))
            found3 = int(groups.group(3))
            if found1 == 24 and found2 == 0 and found3 == 0:
                return '00:' + groups.group(2) + ':' + groups.group(3)
            else:
                return timestring
    except:
        return timestring




if __name__ == "__main__":
    #print returnCorrectedDate("24:00:00")
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    header = lines.first()
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    fromTime = lines.map(lambda x : (x[0],x[2], checkTimeValidity(str(x[2]))))
    toTime = lines.map(lambda x: (x[0], x[4], checkTimeValidity(str(x[4]))))

    invalidFromTime = fromTime.filter(lambda x: x[2] == "INVALID" or x[2] == "NULL")
    invalidFromTime = invalidFromTime.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    invalidFromTime.saveAsTextFile("col2_invalid_data.out")

    validFromTime = fromTime.filter(lambda x: x[1] != "INVALID" and x[2] != "NULL")
    validFromTime = validFromTime.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    validFromTime.saveAsTextFile("col2_valid_data.out")
    #correct data
    validFromTime = fromTime.filter(lambda x: x[2] != "NULL")
    validFromTime = validFromTime.map(lambda x: (x[0], returnCorrectedTime(x[1]), "VALID"))
    validFromTime = validFromTime.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    colName = sc.parallelize(["CMPLNT_NUM \t CMPLNT_FR_TM"])
    sc.union([colName, validFromTime]).saveAsTextFile("col2_corrected.out")

    invalidToTime = toTime.filter(lambda x: x[2] == "INVALID" or x[2] == "NULL")
    invalidToTime = invalidToTime.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    invalidToTime.saveAsTextFile("col4_invalid_data.out")

    validToTime = toTime.filter(lambda x: x[2] != "INVALID" and x[2] != "NULL")
    validToTime = validToTime.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    validToTime.saveAsTextFile("col4_valid_data.out")
    #correct data
    validToTime = toTime.filter(lambda x: x[2] != "NULL")
    validToTime = validToTime.map(lambda x: (x[0], returnCorrectedTime(x[1]), "VALID"))
    validToTime = validToTime.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    colName = sc.parallelize(["CMPLNT_NUM \t CMPLNT_TO_TM"])
    sc.union([colName, validToTime]).saveAsTextFile("col4_corrected.out")

    topInvalidValueFrom = invalidFromTime.map(lambda x:((x[1],x[2]),1)).reduceByKey(lambda a,b:a+b)
    topInvalidValueFrom.saveAsTextFile("col2_statistics.out")
    topInvalidValueTo = invalidToTime.map(lambda x:((x[1],x[2]),1)).reduceByKey(lambda a,b:a+b)
    topInvalidValueTo.saveAsTextFile("col4_statistics.out")
    sc.stop()
