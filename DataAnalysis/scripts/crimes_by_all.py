from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

def get_count_map(col):
	col = col.map(lambda x: x[1]).map(lambda x: (x, 1))
	col_count = col.reduceByKey(add)
	return col_count.map(lambda x: str(x[0]) + "," + str(x[1]))

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_column(sc, 10)
	count_map = get_count_map(col)
	count_map.saveAsTextFile("crimes_A_C.csv")
	col = fetch_column(sc, 11)
	count_map = get_count_map(col)
	count_map.saveAsTextFile("crimes_law_cat.csv")
	col = fetch_column(sc, 13)
	count_map = get_count_map(col)
	count_map.saveAsTextFile("crimes_borough.csv")
	col = fetch_column(sc, 15)
	count_map = get_count_map(col)
	count_map.saveAsTextFile("crimes_loc_occ.csv")
	sc.stop()