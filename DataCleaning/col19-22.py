from pyspark import SparkContext
from operator import add
from helper import *
import sys
from csv import reader 

def check_type(n, c):
	try:
		val = float(n.replace(",", ""))
		if c == 19 or c == 20:
			return val.is_integer()
		return True
	except ValueError:
		return False

def validate(value, col_no):
	flag = False
	reason = "VALID"
	if value == '':
		reason = "NULL"
	elif (not check_type(value, c)):
		reason = "INVALID"
	elif col_no == 19 and ((value < 909900) or (value > 1067600)):
		return "INVALID X-COORDINATE";
	elif col_no == 20 and ((value < 117500) or (value > 275000)):
		return "INVALID Y-COORDINATE";
	elif col_no == 21 and ((value < -75) or (value > -73)):
		return "INVALID LATITUDE";
	elif col_no == 22 and ((value < 40) or (value > 41)):
		return "INVALID LONGITUDE";	
	else:
		flag = True
	return (value, flag, reason)

if __name__ == "__main__":
	sc = SparkContext()
	col_no = [19, 20, 21, 22]
	for c in col_no:
		col = fetch_column(sc, c)
		col_validity_map = col.map(lambda x : (x[0], validate(x[1], c)))
		invalid_col = col_validity_map.filter(lambda x : not x[1][1])
		invalid_col = invalid_col.map(lambda x: x[1])
		invalid_col_out = invalid_col.map(lambda x : str(x[0]) + "\t" + str(x[2]))
		invalid_col_out.saveAsTextFile("col" + str(c) + "_invalid_data.out")
		valid_col = col_validity_map.filter(lambda x : x[1][1])
		valid_col_out = valid_col.map(lambda x : str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][2]))
		valid_col_out.saveAsTextFile("col" + str(c) + "_valid_data.out")
		counts = sc.parallelize(["INVALID COUNT:\t" + str(invalid_col.count()), "VALID COUNT:\t" + str(valid_col.count())]);
		if not invalid_col.isEmpty():
			invalid_count_map = invalid_col.map(lambda x : ((x[0],x[2]), 1)).reduceByKey(add)
			invalid_max_count = invalid_count_map.map(lambda x : x[1]).max()
			invalid_max_occ = invalid_count_map.filter(lambda x : x[1] == invalid_max_count)
			invalid_max_occ_out = invalid_max_occ.map(lambda x : "MAX INVALID OCCURENCE \t" + str(x[0][0]) + "\t" + str(x[0][1]) + "\t" + str(x[1]))
			sc.union([counts, invalid_max_occ_out]).saveAsTextFile("col" + str(c) + "_statistics.out")
		else:
			counts.saveAsTextFile("col" + str(c) + "_statistics.out")
	sc.stop()
