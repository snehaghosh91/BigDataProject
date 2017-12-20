# BigDataProject
Data Cleaning, Data Analysis and Data Exploration

Group Members -
Rakshit Sareen (rs5606)
Saurabh Mahajan (sm6921)	
Sneha Ghosh (sg3533)

Final Project Report - https://docs.google.com/document/d/10eXqXr4Je853OHkP8QWWdvTtgI-MkoKViFRyaKvKYQk/edit#

Google Doc Data Cleaning - https://docs.google.com/document/d/1SmbZb3nXtPsxA4_KVE0zSXGsQbahm659BlG7nZxJiVY/

Crime Dataset Link - https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i

## Part 1 - Data Cleaning
**Steps to Reproduce Results**
1. Download dataset from Crime Dataset Link provided above. Rename file as 'NYPD_Complaint_Data_Historic.csv'.
2. Login to hadoop cluster. Create directory project.
3. Copy csv file from local to the hadoop cluster in project directory -

   **Command**: scp -r NYPD_Complaint_Data_Historic.csv NetID@dumbo.es.its.nyu.edu:/home/NetID/project/ .
   
   Also copy the scripts you want to run to the hadoop cluster.
4. Put the csv file into the Hadoop File system -

   **Command**: hfs -put NYPD_Complaint_Data_Historic.csv
   
5. Running script for individual columns (Eg. for column 10)- 
   
   **Command**: spark-submit --py-files=helper.py col10.py NYPD_Complaint_Data_Historic.csv
   
6. To obtain cleaned csv file with all columns

    **Run script** ./execute.sh

    Then Run **Command**: spark-submit --py-files=helper.py merge.py NYPD_Complaint_Data_Historic.csv
    
7. Cleaned csv can be obtained using below command

   **Command**:hfs -getmerge data.csv cleaned.csv

## Part 2 - Data Analysis
The complete data analysis was performed on the cleaned csv file obtained after data cleaning.

Steps to generate cleaned csv file are mentioned in Part 1 above.

**Pre Requisites** - Python, Pandas, Jupyter Notebook, Matplotlib, Numpy

**Steps to Reproduce Results**
1. Upload the cleaned csv file obtained from Part 1 to the Hadoop cluster-
 
   **Command**: hfs -put cleaned.csv 
   
2. Run scripts to generate data which will be used to plot results-
Scripts can be downloaded from the Data Analysis/scripts folder.
  
  **Command**: spark-submit --py-files=helper.py crimes_by_year_month.py cleaned.csv
  
3. Copy data generated after running the scripts from the hadoop cluster to local 
machine in the Data Analysis/data folder-
  
  **Command**: scp -r NETID@dumbo.es.its.nyu.edu:/home/NETID/project/DataAnalysis/* .
  
4. Start jupyter Notebook from the DataAnalysis folder-
  **Command**: jupyter notebook
  
5. Open any of the ipynb files (eg - crimes_by_year.ipynb) to run and generate plots.

## Part 3 - Data Exploration
1. **Weather** - Crime rate is high during summer as compared to winter.

   Monthly Weather Data in New York  collected from link - http://www.holiday-weather.com/new_york_city/averages
   
   Utilized crimes data over the month generated during analysis to prove hypotheses.
   
2. **Poverty** - Crime rate increases with increase in poverty

   Yearly Poverty Data in New York Boroughs collected from link - http://www1.nyc.gov/site/opportunity/poverty-in-nyc/data-tool.page
   
   Utilized crimes data over the years in New York Boroughs generated during analysis to prove hypotheses.
   
