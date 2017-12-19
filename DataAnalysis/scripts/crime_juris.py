import sys
from operator import add
from pyspark import SparkContext
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
	lines = lines.map(lambda x: ((x[11],x[12]), 1))
	header = lines.first()

	counts = lines.filter(lambda x: x != header).reduceByKey(add) \
			.map(lambda x:(x[1], x[0])).sortByKey(False)

	crimes = ['FELONY', 'MISDEMEANOR', 'VIOLATION']
	for crime in crimes:
		crime_type = counts.filter(lambda x : x[1][0] == crime).take(5)
		offense_juris = sc.parallelize(crime_type).map(formatData)
		offense_juris.saveAsTextFile('crime_'+ crime.lower() +'_juris.csv')
	sc.stop()