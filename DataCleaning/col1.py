from pyspark import SparkContext
from helper import *
from operator import add

if __name__ == "__main__":
    sc = SparkContext()
    col = fetch_column(sc, 0)
    counts  = col.count()
    counts.saveAsTextFile("col1_statistics.out")
    sc.stop()