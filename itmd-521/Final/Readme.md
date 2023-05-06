# ITMD 521 Final Project

This will be the description and deliverable for the final project for ITMD 521

## General Notes

* All commits and code must be placed in the private repo assigned at the beginning of the semester
  * Under the folder: `itmd-521` > `final`
  * Include a *Readme.md* file in the `final` directory with instructions and commands to run each of your scripts
  * Create a sub-folder: `part-one`, `part-two`, `part-three`
  * In each sub-folder provide the PySpark `.py` files to accomplish the requirements
  * I will run this to check your work and compare output
* Clone/pull you code to the spark edge server account you were given access to
* Clone via SSH key - as demonstrated in the video prepared 04/19

* Make use of any of the sample code provided to you in Blackboard
* Make extensive use of the [PySpark API documentation](https://spark.apache.org/docs/latest/api/python/index.html "PySpark API documentation") 
* Make extensive use of the Textbook -- don't go to the internet
* Make extensive use of the Disucssion Board for clarifications and questions

### Part One

This part contains two sub-sections

#### First Section

The first part of the assignment you will be data engineering. You will be converting raw text records, parsing them and saving them in many different formats in our Minio Object storage system.

* Use the raw data set you were assigned
* Use these initial parametersfor each job:
  *  --driver-memory 6G --executor-memory 6G --executor-cores 2 --num-executors 12 --total-executor-cores 24

* Create a PySpark application that will read the `30.txt` from the `itmd521` bucket into a DataFrame
  * Name the PySpark application `XYZ-minio-read-and-process-AA.py`
  * Where XYZ is your initials
  * and AA is the decade you are working on
* In the same PySpark application parse the raw datasource assigned into the assigned 5 outputs and name the results the same prefix as the source (20.txt becomes 20-csv, or 20-csv-lz4, or 20-single-part-csv for example).
  * csv
  * json
  * csv with lz4 compression
  * parquet
  * csv with a single partition (need to adjust filename to not overwrite the first csv requirement)

Steps to execute Part One

 - Login into spark server using credentials: `ssh -i C:\Users\vinut\.ssh\id_ed25519_spark_edge_key vbengaluruprabhudev@system45.rice.iit.edu `
      - Navigate to the folder `/home/vbengaluruprabhudev/vbengaluruprabhudev/itmd-521/Final/part-one`
      - Run below three commands for converting the file to cvs, json, and parquet files

Command do execute:
`spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller VBP-minio-read-and-process-30.py`


#### Second Section

You will continue your data engineering experience in needing to read Raw text file and convert them into CSV and Parquet files. Based on your lastname starting with A-K you will need to convert the decades 30, 40, 70, and 90. Lastname L-Z will do 40, 60, and 80. These files get quite large and may take up to 15 to 48 hours to complete -- don't wait. 

* Create multiple appropriate PySpark files to do the reading
  * Name the PySpark applications in this manner: `minio-read-50.py`
  * Change the last decade number to match
  * Make sure to adjust the `SparkSession.builder.appName()` to be proper
* Save them as CSV and Parquet files in your own assign Minio S3 bucket.
  * Use the same convention as the previous questuion
  * 20-csv
  * 20-parquet

As a hint - do a test run on a small dataset - say 20.txt to see if your logic and bucket permissions are working before starting the larger jobs.

Command to execute minio-read-40.py : `spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller VBP-minio-read-and-process-40.py`

Command to execute minio-read-70.py : ` spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller VBP-minio-read-and-process-70.py`

Command to execute minio-read-90.py : `spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller VBP-minio-read-and-process-90.py`

### Part Two

This part you will read the datasets you created back into your PySpark application and create schemas where necessary.

* Create a single PySpark application
  * Name: `XYZ-minio-read.py` 
* Read your partitioned csv into a DataFrame named: `csvdf`
  * Create a schema based on the sample given in blackboard
* Show the first 10 records and print the schema
* Read your partitioned csv into a DataFrame named: `jsondf`
  * Create a schema based on the sample given in Blackboard
* Show the first 10 records and print the schema
* Read your partitioned csv into a DataFrame named: `parquetdf`
  * Show the first 10 records and print the schema
* Connect to the MariaDB server using the below details in your PySpark application
  * Connect to the database `ncdc` and table `thirties` to read out the dataframe 
  * Show the first 10 records and print the schema
  * Username and Password will be provided to you

python
# Writing out to MySQL your DataFrame results
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.save.html
(splitDF.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","thirty").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save())



Command to execute Part Two: `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 2 --total-executor-cores 24 --driver-memory 8g --proxy-user controller VBP-minio-read.py> ./full-mariadbnewfinal &`



### Part-Three

In this section you will execute the same command 3 times and modify run time parameters and make note of the execution times and *explain* what the adjustments did. To do this create a PySpark application to read your prescribed decade .parquet file data and find all of the weather station IDs that have registered days (count) of visibility less than 200 per year.

Create a file named: `part-three-answer.md` and Using Markdown explain your answer in technical detail from the book. Note - relative statements: its faster, its better, its slower are not correct.

Using these parameters on a reading 50-parquet from your Minio bucket

* `--driver-memory`
* `--executor-memory`
* `--executor-cores`
* `--total-executor-cores`

* First run
  * `--driver-memory 2G --executor-memory 4G --executor-cores 1 --total-executor-cores 20`
  * Your Expectation:
  * Your results/runtime:


Command to execute: `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 20 --total-executor-cores 20 --driver-memory 2g --proxy-user controller VBP-minio-part-three.py > ./Run_first.out &`

* Second run
  * `--driver-memory 10G --executor-memory 12G --executor-cores 2`
  * Your Expectation:
  * Your results/runtime:


Command to execute: `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --driver-memory 10G --executor-memory 12G --executor-cores 2  --proxy-user controller VBP-minio-part-three.py > ./Run_second.out &`

* Third run
  * `--driver-memory 4G --executor-memory 4G --executor-cores 2 --total-executor-cores 40`
  * Your Expectation:
  * Your results/runtime:


Coomand to execute: `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 2 --num-executors 20 --total-executor-cores 40 --driver-memory 4g --proxy-user controller VBP-minio-part-three.py > ./Third_Run.out &`

  

### Part Four

This part you will do some basic analytics using the Spark SQL or the native PySpark libraries. You will use the assigned dataset that you have already processed into the parquet format (load the .parquet file).  Use the Spark optimizations discussed in the previous section to optimize task run time -- resist the temptation to assign all cluster resources - use the first run options as a baseline.  Using the 50.txt or 60.txt depending on your lastname for month of February in each year.

* Using date ranges, select all records for the month of February for each year in the decade, find the following
  * Count the number of records
  * Average air temperature for month of February 
  * Median air temperature for month of February
  * Standard Deviation of air temperature for month of February
  * You may have to add filters to remove records that have values that are legal but not real -- such as 9999
  * Find AVG air temperature per StationID in the month of February
  * Write these value out to a DataFrame and save this to a Parquet file you created in your bucket
    * Name the file `XYZ-part-four-answers-parquet`
    * Will have to construct a schema
    * May want to make use of temp tables to keep smaller sets of data in memory


Coomand to execute: 
`nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 2 --num-executors 20 --total-executor-cores 40 --driver-memory 4g --proxy-user controller VBP-part-four-file.py > ./output-finalpart4 &`



### Final Note

These jobs might take a while to process, potentially hours--*Don't wait!*.  You can execute jobs and add them to the queue -- when resources free up, your job will execute.  You can submit a job to execute without having to keep your computer open all the time by using the `nohup` command, put `nohup` in front of your command and a `&` at the end will background and allow you to disconnect from the spark edge server (not hang up). 

Example: 

nohup spark-submit --master spark://192.168.172.23:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.2" --driver-memory 2G --executor-memory 4G --executor-cores 1 ncdc-single-partition-csv.py 50.txt 50.csv csv &

## Due Date and Finals Presentation

* Submit the URL to your GitHub account to Blackboard by 11:59 pm, Sunday 04/30
  * Worth 80 points
* Thursday May 5th, during the scheduled final exam period you will demonstrate and answer questions about your answers and the results of `part-three` and give a verbal explanation (Use the textbook as your source for explanations)
  * Worth 20 points