import sys
import re
from csv import reader 
from pyspark import SparkContext

# Data Reference for Precincts
# https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page
# Midtown South Precinct - 14, Midtown North Precinct - 18, Central Park Precinct - 22
def get_precincts():
	precincts = [1,5,6,7,9,10,13,14,17,18,19,20,22,23,24,25,26,28,30,32,33,34,40,41,42,43,44,45,46,47,48,49,50,52,60,61,62,63,66,67,68,69,70,71,72,73,75,76,77,78,79,81,83,84,88,90,94,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,120,121,122,123]
	return precincts

# Data Reference for Housing Development
# http://www1.nyc.gov/site/nycha/about/developments.page (Heading - Development Maps)
def get_hadevelopt(sc):
	data = sc.textFile("hadevelopt_ref.txt", 1).map(lambda line: str(line).encode('utf-8').upper())
	return data.collect()

def fetch_lines(sc):
	lines = sc.textFile(sys.argv[1], 1)
	lineList = lines.mapPartitions(lambda x : reader(x))
	header = lineList.first()
	lineList = lineList.filter(lambda x : x != header)
	return lineList

def fetch_column(sc, col_id):
	lineList = fetch_lines(sc)
	col = lineList.map(lambda x : (x[0], x[col_id]))
        return col

def merge_key_values(key_values):
	key = key_values[0]
	values = key_values[1]
	res = [key]
	res.extend(values)
	return res

def check_int(value):
	try:
        	int(value)
        	return True
	except ValueError:
		return False

def find_reason(unique_key, value):
	reason = None
	if not value.strip():
		reason = "EMPTY VALUE"
	else:
		reason = "INVALID"
	return (unique_key, value, reason)

def filter_description(key, desc, cols_ref):
	if key.strip() and key in cols_ref and cols_ref[key] == desc:
		return True
	return False

def validate(value, col_type = None, valid_values = None):
	flag = True
	reason = "VALID"
	if not value.strip():
		reason = "EMPTY VALUE"
                flag = False	
		return (value, flag, reason)
	elif col_type == "CLASSIFICATION CODE":
		if not check_int(value):
			reason = "TYPE INVALID"
			flag = False
		elif not re.match('^[1-9][0-9][0-9]$', value):
			reason = "INVALID"
                	flag = False
	elif col_type == "CRIME_STATUS":
		valid_values = ['ATTEMPTED', 'COMPLETED']	
		if value not in valid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "LAW_CAT":
		valid_values = ['FELONY', 'MISDEMEANOR', 'VIOLATION']	
		if value not in valid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "JURIS_DESC":
		invalid_values = ['OTHER']	
		if value in invalid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "BOROUGH":
		valid_values = ['QUEENS', 'MANHATTAN', 'BRONX', 'STATEN ISLAND', 'BROOKLYN']
		if value not in valid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "PRECINCT":
		if not check_int(value):
			flag = False
			reason = "TYPE INVALID"
		else:
			valid_values = get_precincts()
			if int(value) not in valid_values:
				reason = "INVALID"
				flag = False
	elif col_type == "LOCATION":
		valid_values = ['FRONT OF', 'INSIDE', 'OPPOSITE OF', 'REAR OF', 'OUTSIDE']
		if value not in valid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "PREM_TYPE_DESC":
		invalid_values = ['OTHER']	
		if value in invalid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "HADEVELOPT":
		if value.upper() not in valid_values:
			reason = "INVALID"
			flag = False
	return (value, flag, reason)

