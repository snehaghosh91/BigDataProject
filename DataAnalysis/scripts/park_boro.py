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
	col = fetch_columns(sc, [17,13])
	col = col.filter(lambda x: check_empty(x[0])).map(lambda x: ((x[0], x[1]), 1))
	top_counts = col.reduceByKey(add).map(lambda x: (x[1], x[0])).sortByKey(False)
	last_counts = col.reduceByKey(add).map(lambda x: (x[1], x[0])).sortByKey()
	boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "STATEN ISLAND", "BRONX"]
	for boro in boroughs:
		boro_top = top_counts.filter(lambda x : x[1][1] == boro).take(5)
		boro_last = last_counts.filter(lambda x : x[1][1] == boro).take(5)
		top = sc.parallelize(boro_top).map(lambda x: str(x[1][0]) + "," + str(x[0]))
		top.saveAsTextFile("%s_unsafe_parks.csv" %boro.replace(' ','').lower())
		last = sc.parallelize(boro_last).map(lambda x: str(x[1][0]) + "," + str(x[0]))
		last.saveAsTextFile("%s_safe_parks.csv" %boro.replace(' ','').lower())
	sc.stop()
