# DP_CSV_MYSQL
Implemented data processing pipelline. 
This program loads CSV file into Mysql database.It requires 3 files as input.
1. Data file (CSV file)
2. Datebase connection information (such as database name, username, password in json format)
3. Schema file which has more information about the CSV file and the target table in Mysql database.
  (such as startline from which the data to be loaded, target column datatype, source datatype etc)
The user need to configure schema file and json file. 
The program makes use of Pandas dataframe. Data quality is implemented to check for any null values and incorrect date datatype fields.
Data is cleansed to remove the rows which has more errors such as null values or incorrect datatype.
Finally data is loaded into database.
