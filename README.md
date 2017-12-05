# BigDataProject
Data Cleaning, Data Analysis and Data Exploration

Group Members -
Rakshit Sareen (rs5606)
Saurabh Mahajan (sm6921)	
Sneha Ghosh (sg3533)

Google Doc - https://docs.google.com/document/d/1SmbZb3nXtPsxA4_KVE0zSXGsQbahm659BlG7nZxJiVY/

Crime Dataset Link - https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i

## Part 1 - Data Cleaning
**Steps to Reproduce Results**
1. Download dataset from Crime Dataset Link provided above. Rename file as 'crime.csv'.
2. Login to hadoop cluster. Create directory project.
3. Copy csv file from local to the hadoop cluster in project directory -

   **Command**: scp -r crime.csv NetID@dumbo.es.its.nyu.edu:/home/NetID/project/ .
   
   Also copy the scripts you want to run to the hadoop cluster.
4. Put the csv file into the Hadoop File system -

   **Command**: hfs -put crime.csv
   
5. Running script for individual columns (Eg. for column 10)- 
   
   **Command**: spark-submit --py-files=helper.py col10.py crime_data.csv
   
6. To obtain cleaned csv file with all columns

  **Run script** ./execute.sh
  Then Run **Command**: spark-submit --py-files=helper.py merge.py
