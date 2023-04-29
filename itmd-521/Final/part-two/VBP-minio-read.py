
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

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

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("VBP read part 2 ").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# Read the datatype into a csv dataframe
csvdf = spark.read.csv('s3a://vbengaluruprabhudev/30-single-part-csv')

csvSplitDF = csvdf.withColumn('WeatherStation', csvdf['_c0'].substr(5, 6)) \
.withColumn('WBAN', csvdf['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(csvdf['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', csvdf['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', csvdf['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', csvdf['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', csvdf['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', csvdf['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', csvdf['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', csvdf['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', csvdf['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', csvdf['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', csvdf['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', csvdf['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', csvdf['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', csvdf['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', csvdf['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', csvdf['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', csvdf['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

print("csv dataframe")

csvSplitDF.printSchema()
csvSplitDF.show(10)

# Read the datatype into a json dataframe
jsondf = spark.read.json('s3a://vbengaluruprabhudev/30-csv')

jsonSplitDF = jsondf.withColumn('WeatherStation', jsondf['_c0'].substr(5, 6)) \
.withColumn('WBAN', jsondf['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(jsondf['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', jsondf['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', jsondf['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', jsondf['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', jsondf['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', jsondf['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', jsondf['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', jsondf['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', jsondf['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', jsondf['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', jsondf['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', jsondf['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', jsondf['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', jsondf['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', jsondf['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', jsondf['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', jsondf['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

print("json dataframe")

jsonSplitDF.printSchema()
jsonSplitDF.show(10)

# Read the datatype into a parquet dataframe
parquetdf = spark.read.parquet('s3a://vbengaluruprabhudev/30-csv')

parquetSplitDF = parquetdf.withColumn('WeatherStation', parquetdf['_c0'].substr(5, 6)) \
.withColumn('WBAN', parquetdf['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(parquetdf['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', parquetdf['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', parquetdf['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', parquetdf['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', parquetdf['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', parquetdf['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', parquetdf['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', parquetdf['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', parquetdf['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', parquetdf['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', parquetdf['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', parquetdf['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', parquetdf['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', parquetdf['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', parquetdf['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', parquetdf['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', parquetdf['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

print("parquet dataframe")

parquetSplitDF.printSchema()
parquetSplitDF.show(10)