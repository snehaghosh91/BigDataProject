from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

def generate_csv(borough,bname):
    borough = borough.map(lambda x: (x[1],1));
    borough_map = borough.reduceByKey(lambda x,y : x+y);
    borough_map = borough_map.map(lambda x: str(x[0]) + "," + str(x[1]));
    borough_map.saveAsTextFile("crimes_in_%s_by_LAW_CD.csv" % bname);


if __name__ == "__main__":
    #['MISDEMEANOR', 'FELONY', 'VIOLATION']
    sc = SparkContext();
    lines = sc.textFile(sys.argv[1], 1);
    lineList = lines.mapPartitions(lambda x: reader(x));
    header = lineList.first();
    lineList = lineList.filter(lambda x: x != header);
    lineList = lineList.map(lambda x: (x[13],x[11]));


    manhattan = lineList.filter(lambda x : x[0] == 'MANHATTAN');
    queens = lineList.filter(lambda x: x[0] == 'QUEENS');
    bronx = lineList.filter(lambda x: x[0] == 'BRONX');
    brooklyn = lineList.filter(lambda x: x[0] == 'BROOKLYN');
    staten = lineList.filter(lambda x: x[0] == 'STATEN ISLAND');

    generate_csv(manhattan,"MANHATTAN");
    generate_csv(queens, "QUEENS");
    generate_csv(bronx, "BRONX");
    generate_csv(brooklyn, "BROOKLYN");
    generate_csv(staten, "STATEN_ISLAND");
    sc.stop();