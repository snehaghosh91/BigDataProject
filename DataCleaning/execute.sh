# hfs -rm -r col1_3_invalid_data.out col1_invalid_data.out col2_invalid_data.out col3_invalid_data.out col4_invalid_data.out col5_invalid_data.out col6_invalid_data.out col7_invalid_data.out col8_invalid_data.out col9_invalid_data.out col10_invalid_data.out col11_invalid_data.out col12_invalid_data.out col13_invalid_data.out col14_invalid_data.out col15_invalid_data.out col16_invalid_data.out col17_invalid_data.out col18_invalid_data.out col19_invalid_data.out col20_invalid_data.out col21_invalid_data.out col22_invalid_data.out col23_invalid_data.out
# hfs -rm -r col1_3_valid_data.out col1_valid_data.out col2_valid_data.out col3_valid_data.out col4_valid_data.out col5_valid_data.out col6_valid_data.out col7_valid_data.out col8_valid_data.out col9_valid_data.out col10_valid_data.out col11_valid_data.out col12_valid_data.out col13_valid_data.out col14_valid_data.out col15_valid_data.out col16_valid_data.out col17_valid_data.out col18_valid_data.out col19_valid_data.out col20_valid_data.out col21_valid_data.out col22_valid_data.out col23_valid_data.out
# hfs -rm -r col1_3_statistics.out col1_statistics.out col2_statistics.out col3_statistics.out col4_statistics.out col5_statistics.out col6_statistics.out col7_statistics.out col8_statistics.out col9_statistics.out col10_statistics.out col11_statistics.out col12_statistics.out col13_statistics.out col14_statistics.out col15_statistics.out col16_statistics.out col17_statistics.out col18_statistics.out col19_statistics.out col20_statistics.out col21_statistics.out col22_statistics.out col23_statistics.out
# hfs -rm -r col2_corrected.out col4_corrected.out col7_corrected.out col9_corrected.out 
# hfs -rm -r exactDates.out rangeDates.out endPointDates.out data.csv
spark-submit --py-files=helper.py col1_3.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col2-4.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col6_7.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col8_9.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col10.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col11.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col12.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col13.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col14.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col15.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col16.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col17.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col18.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col19-22.py NYPD_Complaint_Data_Historic.csv
spark-submit --py-files=helper.py col23.py NYPD_Complaint_Data_Historic.csv