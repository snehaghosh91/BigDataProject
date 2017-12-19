import sys
from pyspark import SparkContext
from operator import add
from csv import reader

def formatData(n):
	s = ""
	for d in n:
		if type(d) == int:
			s = str(d)
		else:
			for a in d:
				s += "," + a
	return s

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	lines = lines.map(lambda x: ((x[12],x[13]), 1))
	header = lines.first()

	counts = lines.filter(lambda x: x != header).reduceByKey(add) \
			.map(lambda x:(x[1], x[0])).sortByKey(False)

	boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "STATEN ISLAND", "BRONX"]
	for borough in boroughs:
		boro = counts.filter(lambda x : x[1][1] == borough).take(5)
		juris_boro = sc.parallelize(boro).map(formatData)
		juris_boro.saveAsTextFile('juris_'+ borough.lower().replace(' ','') +'_boro.csv')
	sc.stop()