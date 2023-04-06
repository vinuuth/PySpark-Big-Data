from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

# Removing hard coded password - using os module to import them
import os
import sys

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

#conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
#conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
#conf.set('spark.hadoop.fs.s3a.access.key', "spark521")
#conf.set('spark.hadoop.fs.s3a.secret.key', "79a93eda-ba02-11ec-8a4c-54ee75516ff6")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://10.0.0.50:9000")

#spark = SparkSession.builder.appName("JRH insert 30 to mysql").config('spark.driver.host','192.168.172.45').config(conf=conf).getOrCreate()
spark = SparkSession.builder.appName("VBP read 30").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

df = spark.read.csv('s3a://itmd521/30.txt')

splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
.withColumn('WBAN', df['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

#writeDF = splitDF.coalesce(1)
splitDF.printSchema()
splitDF.show(5)

#writeDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://itmd521/50.csv")
#splitDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://hajek/50.csv")

# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.save.html
splitDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://itmd521/jrh-30-csv")
#(splitDF.write.format("jdbc").option("url","jdbc:mysql://192.168.172.31:3306/ncdc").option("driver","com.mysql.jdbc.Driver").option("dbtable","fifties").option("user",os.getenv('MYSQLUSER')).option("password",os.getenv('MYSQLPASS')).save())
#(splitDF.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","thirty").option("user","worker").option("truncate",True).mode("overwrite").option("password", "").save())