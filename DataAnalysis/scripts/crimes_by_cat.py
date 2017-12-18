from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

if __name__ == "__main__":
        sc = SparkContext()
        col = fetch_columns(sc, [5, 11])
        col_formatted = col.map(lambda x: (dt.strptime(x[0], '%m/%d/%Y').year, x[1]))
        valid_values = ['FELONY', 'MISDEMEANOR', 'VIOLATION']
        for val in valid_values:
                c = col_formatted.filter(lambda x: x[1] == val).map(lambda x: (x[0], 1))
                c = c.reduceByKey(add)
                c.saveAsTextFile("%s_by_year.csv" % val.lower())
