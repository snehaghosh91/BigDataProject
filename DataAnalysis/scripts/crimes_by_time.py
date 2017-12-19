from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

def convert_to_type(x):
	if x in range(4, 12):
		return "morning"
	elif x in range(12, 17):
		return "afternoon"
	elif x in range(17, 21):
		return "evening"
	else:
		return "night"

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_column(sc, 2)
	col_formatted = col.map(lambda x: x[1]).map(lambda x: dt.strptime(x, '%H:%M:%S'))
	col = col_formatted.map(lambda x: convert_to_type(x.hour)).map(lambda x: (x, 1))
	col_count = col.reduceByKey(lambda x,y: x+y)
	col_map = col_count.map(lambda x: str(x[0]) + "," + str(x[1]))
	col_map.saveAsTextFile("crimes_by_time.csv")
