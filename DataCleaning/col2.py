
def labelReportAsExactOrRange(x):
    fromDate = x[0];
    toDate = x[1];

    if toDate.strip() and  not fromDate.strip():
        return "ENDPOINT"
    elif fromDate.strip() and not toDate.strip():
        return "EXACT"
    elif not fromDate.strip() and not toDate.strip():
        return "INVALID"
    else:
        return "RANGE"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    header = lines.first()
    lines = lines.filter(lambda x: x != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    fromandtodate = lines.map(lambda x: (x[1], x[3]))
    labeledDates = fromandtodate.map(lambda x: (labelReportAsExactOrRange(x), 1)).reduceByKey(lambda a, b: a + b)
    print labelReportAsExactOrRange( ('12/31/2015', '') )
    print labelReportAsExactOrRange(('', ''))
    print labelReportAsExactOrRange(('','12/31/2015'))