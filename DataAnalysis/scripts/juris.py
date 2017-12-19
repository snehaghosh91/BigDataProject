import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	lines = lines.map(lambda x: x[12])
	header = lines.first()
	
	counts = lines.filter(lambda x: x != header) \
			.map(lambda x: (x,1)).reduceByKey(add) \
			.map(lambda x: (x[1],x[0])).sortByKey(False) \
			.take(5)
	
	total_juris = sc.parallelize(counts).map(lambda x:','.join(str(d) for d in x))
	
	total_juris.saveAsTextFile('juris.csv')	
	sc.stop()