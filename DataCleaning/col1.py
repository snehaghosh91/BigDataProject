from pyspark import SparkContext


def checkIfInvalid(x):
    if not x.strip():
        return (x,"INVALID")
    return (x,"VALID")

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile("crime.csv")
    header = lines.first()
    lines = lines.filter(lambda x: x != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    col = lines.map(lambda x: x[0])
    col = col.map(lambda x: checkIfInvalid(x))
    col = col.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b : a+b)
    col.saveAsTextFile("col1_statistics.out")
    sc.stop()