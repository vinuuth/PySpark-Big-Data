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
spark = SparkSession.builder.appName("VBP-part-three-and-process-50.py").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()
parquet_file_60 =  "s3a://vbengaluru/50.parquet"
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

parquet_dataframe_50 = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load(parquet_file_60)
parquet_dataframe_50.printSchema()

parquet_dataframe_50.select("WeatherStation", "VisibilityDistance", "ObservationDate").where(col("VisibilityDistance") < 200).groupBy("WeatherStation", "VisibilityDistance", year("ObservationDate")).count().orderBy(desc("VisibilityDistance")).show(10)