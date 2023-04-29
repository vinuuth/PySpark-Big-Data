from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

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
spark = SparkSession.builder.appName("VIN-part-three-second-run").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

file = 's3a://vbengaluruprabhudev/50-parquet'
df = spark.read.format("parquet").load(file)
df.createOrReplaceTempView("PartThreeView")

# weather station IDs that have registered days (count) of visibility less than 200 per year.
lessRegDays = spark.sql(""" SELECT distinct WeatherStation, ObsYear, count(WeatherStation) as CountWeatherStation 
                        FROM 

                        (SELECT distinct ObservationDate, WeatherStation, year(ObservationDate) as ObsYear
                        FROM PartThreeView 
                        WHERE VisibilityDistance < 200
                        group by ObservationDate, WeatherStation)

                        group by WeatherStation, ObsYear
                        ORDER BY CountWeatherStation desc;
                        """
                        )

lessRegDays.show(10)