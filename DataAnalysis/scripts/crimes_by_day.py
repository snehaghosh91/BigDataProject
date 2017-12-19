from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_column(sc, 5)
	weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
	col_formatted = col.map(lambda x: x[1]).map(lambda x: dt.strptime(x, '%m/%d/%Y'))
	col = col_formatted.map(lambda x: x.date().weekday()).map(lambda x: (x, 1))
	col_count = col.reduceByKey(lambda x,y: x+y)
	col_map = col_count.map(lambda x: weekdays[x[0]] + "," + str(x[1]))
	col_map.saveAsTextFile("crimes_by_day.csv")
	sc.stop()
