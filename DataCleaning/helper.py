import sys
import re
from csv import reader 

# Reference for Precincts
# https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page
# Midtown South Precinct - 14, Midtown North Precinct - 18, Central Park Precinct - 22
def get_precincts():
	precincts = [1,5,6,7,9,10,13,14,17,18,19,20,22,23,24,25,26,28,30,32,33,34,40,41,42,43,44,45,46,47,48,49,50,52,60,61,62,63,66,67,68,69,70,71,72,73,75,76,77,78,79,81,83,84,88,90,94,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,120,121,122,123]
	return precincts

def fetch_column(sc, col_id):
	lines = sc.textFile(sys.argv[1], 1)
	line = lines.mapPartitions(lambda x : reader(x))
	header = line.first()
	line = line.filter(lambda x : x != header)
	col = line.map(lambda x : (x[0], x[col_id]))
        return col

def validate(value, col_type = None):
	flag = True
	reason = "VALID"
	if not value.strip():
		reason = "EMPTY VALUE"
                flag = False	
	elif col_type == "CLASSIFICATION CODE":
		if not re.match('^[1-9][0-9][0-9]$', value):
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
	elif col_type == "BOROUGH":
		valid_values = ['QUEENS', 'MANHATTAN', 'BRONX', 'STATEN ISLAND', 'BROOKLYN']
		if value not in valid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "PRECINCT":
		valid_values = get_precincts()
		if int(value) not in valid_values:
			reason = "INVALID"
			flag = False
	elif col_type == "LOCATION":
		valid_values = ['FRONT OF', 'INSIDE', 'OPPOSITE OF', 'REAR OF', 'OUTSIDE']
		if value not in valid_values:
			reason = "INVALID"
			flag = False
	return (value, flag, reason)

