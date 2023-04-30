from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import year
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, col

# Removing hard coded password - using os module to import them
import os
import sys

#defining file to save answer
queryAnswerFile = "s3a://vbengaluruprabhudev/MDG-part-four-answers-parquet"

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

spark = SparkSession.builder.appName("VBP Part four").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

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
countOfRecords = spark.sql(""" SELECT year(ObservationDate) As Year, count(*)
                                    FROM weather
                                    WHERE Month(ObservationDate) = '2'
                                    group by Year
                                    order by Year desc;
                                 """
                                ).show(100)

        
#Average air temperature for month of February
averageAirTemp = spark.sql("""  SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgAirTemperature
                                FROM weather
                                WHERE Month(ObservationDate) = '2'
                                AND AirTemperature < 999 AND AirTemperature > -999
                                group by Year
                                order by Year desc;
                                """
                                ).show(100)

                                   
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
                                ).show(100)

#The weather station ID that has the lowest recorded temperature per year
lowestTempRecorded = spark.sql(""" 
                            SELECT WeatherStation, Year, min_temp
                            FROM(
                            SELECT
                            DISTINCT (WeatherStation),
                            YEAR(ObservationDate) AS Year, 
                            MIN(AirTemperature) OVER (partition by YEAR(ObservationDate)) AS min_temp,
                            ROW_NUMBER() OVER (partition by YEAR(ObservationDate) ORDER BY AirTemperature DESC) AS row_number
                            FROM weather
                            WHERE AirTemperature < 999 AND AirTemperature > -999
                            group by AirTemperature, WeatherStation, Year
                            order by min_temp, row_number asc)
                            WHERE row_number = 1
                            ORDER BY Year;
                            """
                            ).show(20)

#The weather station ID that has the highest recorded temperature per year

highestTempRecorded = spark.sql("""
                            SELECT WeatherStation, Year, max_temp
                            FROM(
                            SELECT
                            DISTINCT (WeatherStation),
                            YEAR(ObservationDate) AS Year, 
                            MAX(AirTemperature) OVER (partition by YEAR(ObservationDate)) AS max_temp,
                            ROW_NUMBER() OVER (partition by YEAR(ObservationDate) ORDER BY AirTemperature ASC) AS row_number
                            FROM weather
                            WHERE AirTemperature < 999 AND AirTemperature > -999
                            group by AirTemperature, WeatherStation, Year
                            order by max_temp, row_number asc)
                            WHERE row_number = 1
                            ORDER BY Year;
                            """
                            ).show(20)

#Removing illegal values form AirTemperature
removeIllegalValues = spark.sql("""
                                SELECT max(WeatherStation) as MaxWeatherStation, YEAR(ObservationDate) as ObservationDate , max(AirTemperature) MaxAirTemperature
                                FROM weather 
                                WHERE WeatherStation !=999999 AND AirTemperature !=999.9 
                                GROUP BY YEAR(ObservationDate) ORDER BY max(AirTemperature);
                                """).show(20)


querySchema7 = StructType([
    StructField('MaxWeatherStation', StringType(), True),
    StructField('ObservationDate', DateType(), True),
    StructField('MaxAirTemperature', FloatType(), True)
])
removeIllegalValues.write.format('parquet').mode('overwrite').save(queryAnswerFile,schema=querySchema7)


querySchema6 = StructType([
    StructField('WeatherStation', StringType(), True),
    StructField('Year', DateType(), True),
    StructField('max_temp', FloatType(), True)
])

highestTempRecorded.write.format('parquet').mode('append').save(queryAnswerFile,schema=querySchema6)

querySchema5 = StructType([
    StructField('WeatherStation', StringType(), True),
    StructField('Year', DateType(), True),
    StructField('min_temp', FloatType(), True)
])

lowestTempRecorded.write.format('parquet').mode('append').save(queryAnswerFile,schema=querySchema5)

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