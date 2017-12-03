import sys
from pyspark import SparkContext
from csv import reader
import re
import datetime


def labelReportAsExactOrRange(x):
    fromDate = x[0]
    toDate = x[1]

    if toDate.strip() and  not fromDate.strip():
        return "ENDPOINT"
    elif fromDate.strip() and not toDate.strip():
        return "EXACT"
    elif not fromDate.strip() and not toDate.strip():
        return "INVALID"
    else:
        return "RANGE"


def returnDateSemantic(datestring):
    if not datestring.strip():
        return "INVALID"
    try:
        datetime.datetime.strptime(datestring, '%m/%d/%Y')
        groups = re.search('(\d{1,2})/(\d{1,2})/(\d{4})', datestring)
        if groups:
            year_str = groups.group(3)
            year_int = int(year_str)
            if year_int < 2006 or year_int > 2016:
                return "INVALID"
            return "VALID"
    except:
        return "FORMATTING ERROR"


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    header = lines.first()
    lines = lines.filter(lambda x: x != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    fromDate = lines.map(lambda x: (x[0],x[1]) )

    toDate = lines.map(lambda x: (x[0],x[3]) )
    rptDate = lines.map(lambda x: (x[0],x[5]))

    rptDate = rptDate.map(lambda x: (x[0],x[1],returnDateSemantic(x[1])))
    validRptDate = rptDate.filter(lambda x: x[2] == "VALID")
    validRptDate = validRptDate.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))

    validRptDate.saveAsTextFile("col5_valid_data.out")
    invalidRptDate = rptDate.filter(lambda x: x[2] == "INVALID")
    invalidRptDate = invalidRptDate.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    invalidRptDate.saveAsTextFile("col5_invalid_data.out")

    # individual filtering on both dates respectively
    fromIndividual = fromDate.map(lambda x: (x[0], x[1], returnDateSemantic(x[1])))
    invalidFromDates = fromIndividual.filter(lambda x:  x[2] == "INVALID")
    invalidFromDates = invalidFromDates.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    validFromDates = fromIndividual.filter(lambda x:  x[2] == "VALID")
    validFromDates = validFromDates.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    #write output to file
    # header = sc.parallelize(["CMPLNT_NUM \t Lat_Lon"])
    invalidFromDates.saveAsTextFile("col1_invalid_data.out")
    validFromDates.saveAsTextFile("col1_valid_data.out")

    toIndividual = toDate.map(lambda x: (x[0], x[1], returnDateSemantic(x[1])))
    invalidToDates = toIndividual.filter(lambda x: x[2] == "INVALID")
    invalidToDates = invalidToDates.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    validToDates = toIndividual.filter(lambda x: x[2] == "VALID")
    validToDates = validToDates.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    #write dates to file
    invalidToDates.saveAsTextFile("col3_invalid_data.out")
    validToDates.saveAsTextFile("col3_valid_data.out")
    #validDates = validFromDates.join(validToDates)
    #individual filtering complete

    #combined filtering for both dates
    fromandtodate = lines.map(lambda x: (x[0],x[1], x[3]))

    result = fromandtodate.map(lambda x: (x[0], x[1], x[2], labelReportAsExactOrRange(x[1:])))
    invalidDates = result.filter(lambda x: x[3] == "INVALID")
    invalidDates = invalidDates.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    invalidDates.saveAsTextFile("col1_3_invalid_data.out")
    validDates = result.filter(lambda x: x[3] != "INVALID")
    validDates = validDates.map(lambda x : str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]))
    validDates.saveAsTextFile("col1_3_valid_data.out")

    #filtering dates based on semantics and writing individual files
    exactDates = result.filter(lambda x: x[3] == "EXACT")
    exactDates.saveAsTextFile("exactDates.out")
    rangeDates = result.filter(lambda x: x[3] == "RANGE")
    rangeDates.saveAsTextFile("rangeDates.out")
    endPointDates = result.filter(lambda x: x[3] == "ENDPOINT")
    endPointDates.saveAsTextFile("endPointDates.out")
    sc.stop()