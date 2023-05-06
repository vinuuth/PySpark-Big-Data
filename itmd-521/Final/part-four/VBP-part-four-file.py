from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructType, StructField,DateType, FloatType, DoubleType, StringType
from pyspark.sql.functions import to_date, func
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import year
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, col,avg

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

spark = SparkSession.builder.appName("VBP- part-four").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

structSchema = StructType([StructField('WeatherStation', StringType(), True),
StructField('WBAN', StringType(), True),
StructField('ObservationDate',  DateType(), True),
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
StructField('ATQualityCode', IntegerType(), True),
StructField('DewPoint', FloatType(), True),
StructField('DPQualityCode', IntegerType(), True),
StructField('AtmosphericPressure', FloatType(), True),
StructField('APQualityCode', IntegerType(), True)])

parquetdf = spark.read.parquet("s3a://vbengaluruprabhudev/50-parquet", header=True, schema=structSchema)   
parquetdf.createOrReplaceTempView("weather")


#Count the number of records
cntOfRecords = spark.sql(""" SELECT year(ObservationDate) As Year, count(*) As NoOfRecords
                                    FROM weather
                                    WHERE Month(ObservationDate) = '2'
                                    group by Year
                                    order by Year desc;
                                 """
                                )
cntOfRecords.show(20)

#Average air temperature for month of February
avgAirTemp = spark.sql("""  SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgAirTemperature
                                FROM weather
                                WHERE Month(ObservationDate) = '2'
                                AND AirTemperature < 999 AND AirTemperature > -999
                                group by Year
                                order by Year desc;
                                """
                                )
avgAirTemp.show(20)
#Standard Deviation of air temperature for month of February
stdAirTemp = spark.sql(""" SELECT year(ObservationDate) As Year, std(AirTemperature) as Standard_deviation
                                    FROM weather
                                    WHERE Month(ObservationDate) = '2'
                                    AND AirTemperature < 999 AND AirTemperature > -999
                                    group by Year
                                    order by Year desc;
                                 """
                                )
stdAirTemp.show(20)

#Find AVG air temperature per StationID in the month of February

stationIdFeb = parquetdf.filter(month("ObservationDate") == 2)
avgTemperature = stationIdFeb.groupBy("WeatherStation").agg(avg("AirTemperature").alias("AverageTemperature"))

avgTemperature.show(20)

#Median air temperature for month of February
#mediAirTemp = parquetdf.approxQuantile('AirTemperature', [0.5], 0.25)
#print(f"Median air temmp:{mediAirTemp}")
#mediAirTemp.show(20)
# Calculate median air temperature
median_temp = parquetdf.filter(month("ObservationDate") == 2) \
                .groupBy(year("ObservationDate").alias("Year")) \
                .agg(func.percentile_approx("AirTemperature",0.5).alias("Median_Air_Temp"))
median_temp.show(10)

#Removing illegal values form AirTemperature
removeIllegalValues = spark.sql("""
                                SELECT max(WeatherStation) as MaxWeatherStation, YEAR(ObservationDate) as ObservationDate , max(AirTemperature) MaxAirTemperature
                                FROM weather 
                                WHERE WeatherStation !=999999 AND AirTemperature !=999.9 
                                GROUP BY YEAR(ObservationDate) ORDER BY max(AirTemperature);
                                """).show(20)


cntOfRecords.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-count-parquet")

avgAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-avg-parquet")

stdAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-std-parquet")

avgTemperature.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-station-parquet")

median_temp.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-median-parquet")

