from pyspark import SparkContext
from csv import reader

res = ''
def create_csv(x, flag):
	global res
	if flag == 1:
		res = ''
	if not type(x) is tuple:
		return
	for n in x:
		if type(n) is tuple:
			create_csv(n, 0)
		else:
			if res == '':
				res = str(n)
			else:
				res = res + ',"' + str(n) + '"'
	return res
def reorder(x):
	y = x.split(",")
	y[2],y[3]=y[3],y[2]
	return ",".join(y)
if __name__ == "__main__":
	sc = SparkContext()
	header = sc.parallelize(["CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon"])
	for i in range(1,24):
		if i == 3:
			continue
		if i == 1:
			lines = sc.textFile("col1_3_valid_data.out", 1)
		elif i == 7 or i == 9 or i == 4 or i == 2:
			lines = sc.textFile("col"+ str(i) +"_corrected.out", 1)
		else:
			lines = sc.textFile("col"+ str(i) +"_valid_data.out", 1)
		if i == 1:
			lines = lines.map(lambda x:(x.split('\t')[0],(x.split('\t')[1],x.split('\t')[2])))
			data = lines
		else:
			lines = lines.map(lambda x:(x.split('\t')[0],x.split('\t')[1]))
			data = data.join(lines)
		if i == 23:
			data = data.map(lambda x: create_csv(x, 1))
			data = data.map(lambda x: reorder(x))
			sc.union([header, data]).saveAsTextFile("data.csv")