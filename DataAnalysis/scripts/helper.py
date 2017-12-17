import sys
from csv import reader 
from pyspark import SparkContext

def fetch_lines(sc):
	lines = sc.textFile(sys.argv[1], 1)
	lineList = lines.mapPartitions(lambda x : reader(x))
	header = lineList.first()
	lineList = lineList.filter(lambda x : x != header)
	return lineList

def fetch_column(sc, col_id):
	lineList = fetch_lines(sc)
	col = lineList.map(lambda x : (x[0], x[col_id]))
	return col
