from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_columns(sc, [21, 22, 13])
	boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "STATEN ISLAND", "BRONX"]
	for boro in boroughs:
		boro_data = col.filter(lambda x : x[2] == boro)
		boro_data = boro_data.map(lambda x: str(x[0]) + "," + str(x[1]))
		boro_data.saveAsTextFile("%s_latlong.csv" %boro.replace(' ','').lower())
	sc.stop()
