from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructType, StructField,DateType, FloatType, DoubleType, StringType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import year
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, col,avg

# Removing hard coded password - using os module to import them
import os
import sys

#defining file to save answer
#queryAnswerFile = "s3a://vbengaluruprabhudev/VBP-part-four-answers-parquet"

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

spark = SparkSession.builder.appName("VBP-part-four").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

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
countOfRecords = spark.sql(""" SELECT year(ObservationDate) As Year, count(*) As NoOfRecords
                                    FROM weather
                                    WHERE Month(ObservationDate) = '2'
                                    group by Year
                                    order by Year desc;
                                 """
                                )
countOfRecords.show(20)

#Average air temperature for month of February
averageAirTemp = spark.sql("""  SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgAirTemperature
                                FROM weather
                                WHERE Month(ObservationDate) = '2'
                                AND AirTemperature < 999 AND AirTemperature > -999
                                group by Year
                                order by Year desc;
                                """
                                )
averageAirTemp.show(20)

                                   
#Median air temperature for month of February
medianAirTemp = parquetdf.approxQuantile('AirTemperature', [0.5], 0.25)
print(f"Median air temmp:{medianAirTemp}")

#Standard Deviation of air temperature for month of February
standardAirTemp = spark.sql(""" SELECT year(ObservationDate) As Year, std(AirTemperature) as Standard_deviation
                                    FROM weather
                                    WHERE Month(ObservationDate) = '2'
                                    AND AirTemperature < 999 AND AirTemperature > -999
                                    group by Year
                                    order by Year desc;
                                 """
                                )
standardAirTemp.show(20)

#Find AVG air temperature per StationID in the month of February

stationIdFeb = parquetdf.filter(month("ObservationDate") == 2)
averageTemperature = stationIdFeb.groupBy("WeatherStation").agg(avg("AirTemperature").alias("AverageTemperature"))

averageTemperature.show(20)

#Removing illegal values form AirTemperature
removeIllegalValues = spark.sql("""
                                SELECT max(WeatherStation) as MaxWeatherStation, YEAR(ObservationDate) as ObservationDate , max(AirTemperature) MaxAirTemperature
                                FROM weather 
                                WHERE WeatherStation !=999999 AND AirTemperature !=999.9 
                                GROUP BY YEAR(ObservationDate) ORDER BY max(AirTemperature);
                                """).show(20)


countOfRecords.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-count-parquet")

averageAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-avg-parquet")

standardAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-std-parquet")

averageTemperature.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-answers-station-parquet")

#medianAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-median-air-temp")

removeIllegalValues.write.format("parquet").mode("overwrite").parquet("s3a://vbengaluruprabhudev/VBP-part-four-remove-invalid-values")