from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

def generate_csv(col, year_or_month):
	if year_or_month == "year":
		col = col.map(lambda x: (x.year, 1))
	else:	
		col = col.map(lambda x: (x.month, 1))
	col_count = col.reduceByKey(lambda x,y: x+y)
	col_map = col_count.map(lambda x: str(x[0]) + "," + str(x[1]))
	col_map.saveAsTextFile("crimes_by_%s.csv" % year_or_month)

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_column(sc, 5)
	col_formatted = col.map(lambda x: x[1]).map(lambda x: dt.strptime(x, '%m/%d/%Y'))
	generate_csv(col_formatted, "year")
	generate_csv(col_formatted, "month")
