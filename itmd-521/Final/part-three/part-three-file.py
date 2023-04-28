from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import desc

# Removing hard coded password - using os module to import them
import os
import sys

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("VIN-read-50-parquet-firstrun").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()


parquet_file = "s3a://vbengaluruprabhudev/50-parquet"
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

dataFrame = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load(parquet_file)
dataFrame.printSchema()

#find all of the weather station IDs that have registered days (count) of visibility less than 200 per year.
query1 = dataFrame.select("WeatherStation","VisibilityDistance","ObservationDate").where((col("VisibilityDistance") < 200) & (col("WeatherStation") != 999999)).groupBy("WeatherStation","VisibilityDistance",year("ObservationDate")).count().orderBy(desc(year("ObservationDate")))
query1.show(10)