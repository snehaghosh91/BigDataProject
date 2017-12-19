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

def get_col_values(row, col_ids):
	out = []
	for col_id in col_ids:
		out.append(row[col_id])
	return tuple(out)

def fetch_columns(sc, col_ids):
	lineList = fetch_lines(sc)
	col = lineList.map(lambda x : get_col_values(x, col_ids))
	return col
