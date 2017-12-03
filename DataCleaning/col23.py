from pyspark import SparkContext
from operator import add
from csv import reader 
import sys

def check_type_value(lat, lon, n):
	x = n[1:-1].split(',')
	try:
		nlat = float(x[0])
		nlon = float(x[1])
		if float(lat) == nlat and float(lon) == nlon:
			return True
		else:
			return False
	except ValueError:
		return False

def validate(lat, lon, value):
	flag = False
	reason = "VALID"
	if value == '':
		reason = "NULL"
	elif (not check_type_value(lat, lon, value)):
		reason = "INVALID"
	else:
		flag = True
	return (value, flag, reason)

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	line = lines.mapPartitions(lambda x : reader(x))
	header = line.first()
	line = line.filter(lambda x : x != header)
	col = line.map(lambda x : (x[0], x[21], x[22], x[23]))
	col_validity_map = col.map(lambda x : (x[0], validate(x[1], x[2], x[3])))
	invalid_col = col_validity_map.filter(lambda x : not x[1][1])
	invalid_col_out = invalid_col.map(lambda x : str(x[0]) + "\t" + str(x[2]))
	header = sc.parallelize(["CMPLNT_NUM \t Lat_Lon"])
	sc.union([header, invalid_col_out]).saveAsTextFile("col23_invalid_data.out")
	valid_col = col_validity_map.filter(lambda x : x[1][1])
	valid_col_out = valid_col.map(lambda x : str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][2]))
	sc.union([header, valid_col_out]).saveAsTextFile("col23_valid_data.out")
	counts = sc.parallelize(["INVALID COUNT:\t" + str(invalid_col.count()), "VALID COUNT:\t" + str(valid_col.count())]);
	if not invalid_col.isEmpty():
		invalid_col = invalid_col.map(lambda x: x[1])
		invalid_count_map = invalid_col.map(lambda x : ((x[0],x[2]), 1)).reduceByKey(add)
		invalid_max_count = invalid_count_map.map(lambda x : x[1]).max()
		invalid_max_occ = invalid_count_map.filter(lambda x : x[1] == invalid_max_count)
		invalid_max_occ_out = invalid_max_occ.map(lambda x : "MAX INVALID OCCURRENCE \t" + str(x[0][0]) + "\t" + str(x[0][1]) + "\t" + str(x[1]))
		sc.union([counts, invalid_max_occ_out]).saveAsTextFile("col23_statistics.out")
	else:
		counts.saveAsTextFile("col23_statistics.out")
	sc.stop()
