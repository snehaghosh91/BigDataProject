from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_columns(sc, [5, 7])
	col_formatted = col.map(lambda x: (dt.strptime(x[0], '%m/%d/%Y').year, x[1]))
	top_values = col_formatted.map(lambda x: (x[1], 1))
	top_values = sorted(top_values.reduceByKey(add).collect(), key = lambda x: x[1], reverse=True)[:5]
	for i in range(len(top_values)):
		top_values[i] = top_values[i][0]
	index = 0
	for val in top_values:
		index += 1
		c = col_formatted.filter(lambda x: x[1] == val).map(lambda x: (x[0], 1))
		c = c.reduceByKey(add)
		c = c.map(lambda x: str(x[0]) + "," + str(x[1]) + "," + str(val))
		c.saveAsTextFile("ofns_by_year_%s.csv" % index)
	sc.stop()
