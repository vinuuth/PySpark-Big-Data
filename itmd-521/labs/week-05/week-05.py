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


    divschema = StructType([structField('trip_id', IntegerType(), True)
                 structField('starttime', StringType(), True)
                 structField('stoptime', StringType(), True)
                 structField('bikeid', IntegerType(), True)
                 structField('tripduration', IntegerType(), True)
                 structField('from_station_id', IntegerType(), True)
                 structField('from_staion_name', StringType(), True)
                 structField('to_station_id', IntegerType(), True)
                 structField('to_station_name', StringType(), True)
                 structField('usertype', StringType(), True)
                 structField('gender', StringType(), True)
                 structField('birthyear', IntegerType(), True)})

    # reading csv file
    divy_df = spark.read.csv(div, header=True, schema=divschema)

    #records display

    divy_df.show()

    #print schema

    print(divy_df.printSchema())
    print ("Total rows = %d" % (div_df.count()))

    # DDL Schema

    ddlschema = "trip_id INT, stattime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    ddl_df = spark.read.csv(div, header=True, schema=divschema)

    ddl_df.show()

    print(ddl_df.printSchema())
    print ("Total rows = %d" % (div_df.count()))

    spark.stop()
