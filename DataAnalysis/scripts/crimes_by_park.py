from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

def check_empty(x):
	if x.strip():
		return True
	else:
		return False

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_columns(sc, [17])
	col = col.filter(lambda x: check_empty(x[0])).map(lambda x: (x[0], 1))
	top_values = col.reduceByKey(add).map(lambda x: (x[1], x[0])).sortByKey(False)
	last_values = col.reduceByKey(add).map(lambda x: (x[1], x[0])).sortByKey()
	top = top_values.take(10)
	last = last_values.take(10)
	top = sc.parallelize(top).map(lambda x: str(x[1]) + "," + str(x[0]))
	top.saveAsTextFile("unsafe_parks.csv")
	last = sc.parallelize(last).map(lambda x: str(x[1]) + "," + str(x[0]))
	last.saveAsTextFile("safe_parks.csv")
	sc.stop()
