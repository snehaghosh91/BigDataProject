from pyspark import SparkContext
from helper import *
from operator import add

def process_col6(sc):
	col = fetch_column(sc, 6)
	col_validity_map = col.map(lambda x :
		(x[0], validate(x[1], "CLASSIFICATION CODE")))
	invalid_col = col_validity_map.filter(lambda x : not x[1][1])
	invalid_col_out = invalid_col.map(
		lambda x : str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][2]))
	header = sc.parallelize(["CMPLNT_NUM \t KY_CD"])
	sc.union([header, invalid_col_out]).saveAsTextFile("col6_invalid_data.out")
	valid_col = col_validity_map.filter(lambda x : x[1][1]) \
				.map(lambda x: (x[0], x[1][0]))
	valid_col_out = valid_col.map(lambda x : str(x[0]) + "\t" + str(x[1]))
	sc.union([header, valid_col_out]).saveAsTextFile("col6_valid_data.out")
	counts = sc.parallelize(
		["INVALID COUNT:\t" + str(invalid_col.count()),
		"VALID COUNT:\t" + str(valid_col.count())])
	if not invalid_col.isEmpty():
		invalid_col = invalid_col.map(lambda x: x[1])
		invalid_count_map = invalid_col.map(lambda x : ((x[0],x[2]), 1)).reduceByKey(add)
		invalid_max_count = invalid_count_map.map(lambda x : x[1]).max()
		invalid_max_occ = invalid_count_map.filter(lambda x : x[1] == invalid_max_count)
		invalid_max_occ_out = invalid_max_occ.map(
			lambda x : "MAX INVALID OCCURRENCE \t" + str(x[0][0]) + "\t" +
				str(x[0][1]) + "\t" + str(x[1]))
		sc.union([counts, invalid_max_occ_out]).saveAsTextFile("col6_statistics.out")
	else:
		counts.saveAsTextFile("col6_statistics.out")
	return col

def process_col7(sc, col6_valid):
	col7 = fetch_column(sc, 7)
	merged_cols = col6_valid.join(col7).map(merge_key_values)
	merged_cols_without_key = merged_cols.map(lambda x: (x[1], x[2]))
	cols_count = merged_cols_without_key.map(lambda x: (x,1)) \
					.reduceByKey(lambda x, y : x+y)
	cols_sorted = cols_count.sortByKey()
	cols_max = cols_count.filter(lambda x: x[0][1] != '') \
			.map(lambda x: (x[0][0],(x[0][1], x[1]))) \
			.reduceByKey(lambda x, y: x if x[1] >= y[1] else y)
	cols_ref = cols_max.map(lambda x: (x[0], x[1][0])).collectAsMap()
	output = cols_sorted.map(lambda x: '%s\t%s\t%s' % (x[0][0],x[0][1],x[1]))
	header = sc.parallelize(["CMPLNT_NUM \t OFNS_DESC"])
	valid_col = merged_cols.filter(lambda x: filter_description(x[1], x[2], cols_ref))
	valid_col_out = valid_col.map(lambda x : str(x[0]) + "\t" + str(x[2]))
	sc.union([header, valid_col_out]).saveAsTextFile("col7_valid_data.out")
	invalid_col = merged_cols.filter(lambda x: not filter_description(x[1], x[2], cols_ref))
	invalid_col_reason = invalid_col.map(lambda x : find_reason(x[0], x[2]))
	invalid_col_out = invalid_col_reason.map(
		lambda x : str(x[0]) + "\t" + str(x[2]))
	sc.union([header, invalid_col_out]).saveAsTextFile("col7_invalid_data.out")
	corrected_col = invalid_col.map(lambda x: (x[0], x[1], cols_ref[x[1]]))
	corrected_col_out = corrected_col.map(lambda x : str(x[0]) + "\t" + str(x[2]))
	sc.union([header, valid_col_out, corrected_col_out]).saveAsTextFile("col7_corrected.out")
	counts = sc.parallelize(["INVALID COUNT:\t" + str(invalid_col.count()),
				"VALID COUNT:\t" + str(valid_col.count())])
	if not invalid_col.isEmpty():
		invalid_count_map = invalid_col_reason.map(lambda x : ((x[1],x[2]), 1)) \
						.reduceByKey(add)
		invalid_max_count = invalid_count_map.map(lambda x : x[1]).max()
		invalid_max_occ = invalid_count_map.filter(lambda x : x[1] == invalid_max_count)
		invalid_max_occ_out = invalid_max_occ.map(
			lambda x : "MAX INVALID OCCURRENCE \t" +
				 str(x[0][0]) + "\t" + str(x[0][1]) + "\t" + str(x[1]))
		sc.union([counts, invalid_max_occ_out]).saveAsTextFile("col7_statistics.out")
	else:
		counts.saveAsTextFile("col7_statistics.out")


if __name__ == "__main__":
	sc = SparkContext()
	col6 = process_col6(sc)
	process_col7(sc, col6)
	sc.stop()
