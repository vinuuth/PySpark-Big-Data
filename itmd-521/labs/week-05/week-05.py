from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = (SparkSession
        .builder
        .appName("DivyPy")
        .getOrCreate())
    # get the M&M data set file name

    div = "Divvy_Trips_2015-Q1.csv"
    # read the file into a Spark DataFrame
    div_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(div))
    div_df.show()

    print(div_df.printSchema())

    print("Total rows = %d" % (div_df.count()))


    divschema =  StructType([StructField('trip_id', IntegerType(), True),
                 StructField('starttime', StringType(), True),
                 StructField('stoptime', StringType(), True),
                 StructField('bikeid', IntegerType(), True),
                 StructField('tripduration', IntegerType(), True),
                 StructField('from_station_id', IntegerType(), True),
                 StructField('from_staion_name', StringType(), True),
                 StructField('to_station_id', IntegerType(), True),
                 StructField('to_station_name', StringType(), True),
                 StructField('usertype', StringType(), True),
                 StructField('gender', StringType(), True),
                 StructField('birthyear', IntegerType(), True)])

    # reading csv file
    divy_df = spark.read.csv(div, header=True, schema=divschema)

    #records display

    divy_df.show()

    #print schema

    print(divy_df.printSchema())
    print(divy_df.schema)
    print ("Total rows = %d" % (divy_df.count()))

    # DDL Schema

    ddlschema = "trip_id INT, stattime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    ddl_df = spark.read.csv(div, header=True, schema=ddlschema)

    ddl_df.show()

    print(ddl_df.printSchema())
    print ("Total rows = %d" % (ddl_df.count()))

    spark.stop()
