from pyspark import SparkContext
from helper import *
from operator import add
from datetime import datetime as dt

def generate_csv(borough, year_or_month,bname):
    if year_or_month == "year":
        borough = borough.map(lambda x: (x[0].year,1));
    else:
        borough = borough.map(lambda x: (x.month, 1));

    borough_map = borough.reduceByKey(lambda x,y : x+y);
    borough_map = borough_map.map(lambda x: str(x[0]) + "," + str(x[1]));
    borough_map.saveAsTextFile("crimes_by_%s_per_year.csv" % bname);


if __name__ == "__main__":
    #['MISDEMEANOR', 'FELONY', 'VIOLATION']
    sc = SparkContext();
    lines = sc.textFile(sys.argv[1], 1);
    lineList = lines.mapPartitions(lambda x: reader(x));
    header = lineList.first();
    lineList = lineList.filter(lambda x: x != header);
    lineList = lineList.map(lambda x: ( dt.strptime(x[5], '%m/%d/%Y'),x[13],x[11]));
    manhattan = lineList.filter(lambda x : x[1] == 'MANHATTAN');
    queens = lineList.filter(lambda x: x[1] == 'QUEENS');
    bronx = lineList.filter(lambda x: x[1] == 'BRONX');
    brooklyn = lineList.filter(lambda x: x[1] == 'BROOKLYN');
    staten = lineList.filter(lambda x: x[1] == 'STATEN ISLAND');

    generate_csv(manhattan,"year","MANHATTAN");
    generate_csv(queens, "year", "QUEENS");
    generate_csv(bronx, "year", "BRONX");
    generate_csv(brooklyn, "year", "BROOKLYN");
    generate_csv(staten, "year", "STATEN_ISLAND");

    #col = fetch_column(sc, 14)
	#col_formatted = col.map(lambda x: x[1]).map(lambda x: dt.strptime(x, '%m/%d/%Y'))
	#generate_csv(col_formatted, "year")
	#generate_csv(col_formatted, "month")

    #filter out year from borough date and make RDDS;
