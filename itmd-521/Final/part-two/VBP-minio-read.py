from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
# Removing hard coded password - using os module to import them
import os
import sys
 
# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")
 
spark = SparkSession.builder.appName("VBP-part-two-MariaDB").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()
 
#Create a schema

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")    
structSchema =  StructType([StructField('WeatherStation', StringType(), True),
                    StructField('WBAN', StringType(), True),
                    StructField('ObservationDate', DateType(), True),
                    StructField('ObservationHour', IntegerType(), True),
                    StructField('Latitude', FloatType(), True),
                    StructField('Longitude', FloatType(), True),
                    StructField('Elevation', IntegerType(), True),
                    StructField('WindDirection', IntegerType(), True),
                    StructField('WDQualityCode', IntegerType(), True),
                    StructField('SkyCeilingHeight', IntegerType(), True),
                    StructField('SCQualityCode', IntegerType(), True),
                    StructField('VisibilityDistance', IntegerType(), True),
                    StructField('VDQualityCode', IntegerType(), True),
                    StructField('AirTemperature', FloatType(), True),
                    StructField('ATQualityCode', FloatType(), True),
                    StructField('DewPoint', FloatType(), True),
                    StructField('DPQualityCode', DoubleType(), True),
                    StructField('AtmosphericPressure', FloatType(), True)])

dataFrame = spark.read.csv("s3a://vbengaluruprabhudev/30-csv", header=True, schema=structSchema)
 
csvdf = dataFrame

csvdf.na.drop()  
print("CSV Schema")
csvdf.printSchema()
print("Display data - csv")
csvdf.show(10)
 
#Converting to json 
dataFrame.write.format("json").option("header", "true").mode("overwrite").save("s3a://vbengaluruprabhudev/30-json-outfile")
jsondf = spark.read.schema(structSchema).json("s3a://vbengaluruprabhudev/30-json-outfile")
print("JSON Schema")
jsondf.printSchema()
print("Display data - json")
jsondf.show(10)
 
#Converting to parquet
dataFrame.write.format("parquet").option("header", "true").mode("overwrite").save("s3a://vbengaluruprabhudev/30-parquet-outfile")
parquetdf = spark.read.schema(structSchema).parquet("s3a://vbengaluruprabhudev/30-parquet-outfile")
print("Parquet Schema")
parquetdf.printSchema()
print("Display data - parquet")
parquetdf.show(10)


#loading parrquet dataframe to Maria DB
mariaDF = parquetdf
mariaDF.printSchema()
mariaDF.show(10)

<<<<<<< HEAD
print("---------------Converting to PARQUET ---------------------------")
cadf.write.format("parquet").option("header", "true").mode("overwrite").save("s3a://vbengaluruprabhudev/30-parttwo-parquet")
parquetdf = spark_session.read.schema(schema).parquet("s3a://vbengaluruprabhudev/30-part2-parquet")
#Printschema
print("Print PARQUET Schema")
parquetdf.printSchema()
#Displayschema
print("Display PARQUETDF")
parquetdf.show(10)
=======
mariaDF.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBP-thirties").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()
>>>>>>> 583ceef959558db5997f32d159db1b82ffcf1223

readDF = mariaDF.read.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBP-thirties").option("user",os.getenv("MYSQLUSER")).option("truncate",True).mode("overwrite").option("password", os.getenv("MYSQLPASS")).load()

readDF.show(10)

<<<<<<< HEAD
read_df.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBP-thirties").option("user",os.getenv("MYSQLUSER")).option("truncate",True).mode("overwrite").option("password", os.getenv("MYSQLPASS")).save()


jdbcDF = read_df.read.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBP-thirties").option("user",os.getenv("MYSQLUSER")).option("truncate",True).mode("overwrite").option("password", os.getenv("MYSQLPASS")).load()

jdbcDF.show(10)

print(jdbcDF.schema)
querySchema4 = StructType([
    StructField('Year', DateType(), True),
    StructField('Standard_deviation', FloatType(), True)
])

standardAirTemp.write.format('parquet').mode('append').save(queryAnswerFile,schema=querySchema4)

querySchema3 = StructType([
   StructField('Year', DateType(), True),
   StructField('Standard_deviation', FloatType(), True)
])

averageAirTemp.write.format('parquet').mode('append').save(queryAnswerFile,schema=querySchema3)

querySchema2=StructType([
StructField('Year', DateType(), True),
StructField('Count(1)', IntegerType(), True),
])

countOfRecords.write.format('parquet').mode('append').save(queryAnswerFile,schema=querySchema2)

print(jdbcDF.schema)

=======
readDF.printSchema()
>>>>>>> 583ceef959558db5997f32d159db1b82ffcf1223
