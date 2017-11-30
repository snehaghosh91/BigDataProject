from pyspark import SparkContext
from helper import *
from operator import add

if __name__ == "__main__":
	sc = SparkContext()
	col = fetch_column(sc, 14)
	#col.distinct().saveAsTextFile("col14_distinct.out")
	col_validity_map = col.map(lambda x : (x[0], validate(x[1], "PRECINCT")))
	invalid_col = col_validity_map.filter(lambda x : not x[1][1])
	invalid_col_out = invalid_col.map(lambda x : str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][2]))
	header = sc.parallelize(["CMPLNT_NUM \t ADDR_PCT_CD"])
	sc.union([header, invalid_col_out]).saveAsTextFile("col14_invalid_data.out")
	valid_col = col_validity_map.filter(lambda x : x[1][1])
	valid_col_out = valid_col.map(lambda x : str(x[0]) + "\t" + str(x[1][0]))
	sc.union([header, valid_col_out]).saveAsTextFile("col14_valid_data.out")
	counts = sc.parallelize(["INVALID COUNT:\t" + str(invalid_col.count()), "VALID COUNT:\t" + str(valid_col.count())]);
        if not invalid_col.isEmpty():
		invalid_col = invalid_col.map(lambda x: x[1])
		invalid_count_map = invalid_col.map(lambda x : ((x[0],x[2]), 1)).reduceByKey(add)
		invalid_max_count = invalid_count_map.map(lambda x : x[1]).max()
		invalid_max_occ = invalid_count_map.filter(lambda x : x[1] == invalid_max_count)
		invalid_max_occ_out = invalid_max_occ.map(lambda x : "MAX INVALID OCCURENCE \t" + str(x[0][0]) + "\t" + str(x[0][1]) + "\t" + str(x[1]))
		sc.union([counts, invalid_max_occ_out]).saveAsTextFile("col14_statistics.out")
	else:
		counts.saveAsTextFile("col14_statistics.out")
	sc.stop()
