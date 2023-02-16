package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object week05 {
  def main(args: Array[String]){
    val spark = (SparkSession
        .builder
        .appName("DivyPy")
        .getOrCreate())
  


    val div = "Divvy_Trips_2015-Q1.csv"

    val div_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(div))
    div_df.show()

    print(div_df.printSchema())

    println(s"Total Rows = ${div_df.count()}")


   val divschema =  StructType(Array(StructField("trip_id", IntegerType, True),
                 StructField("starttime", StringType, True),
                 StructField("stoptime", StringType(), True),
                 StructField("bikeid", IntegerType(), True),
                 StructField("tripduration", IntegerType(), True),
                 StructField("from_station_id", IntegerType(), True),
                 StructField("from_staion_name", StringType(), True),
                 StructField("to_station_id", IntegerType(), True),
                 StructField("to_station_name", StringType(), True),
                 StructField("usertype", StringType(), True),
                 StructField("gender", StringType(), True),
                 StructField("birthyear", IntegerType(), True))g)

    divy_df = spark.read.schema(divschema).csv(div)

    divy_df.show()

    #print schema

    print(divy_df.printSchema())
    print(divy_df.schema)
    println(s"Total Rows = ${divy_df.count()}")



    ddlschema = "trip_id INT, stattime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    ddl_df = spark.read.schema(ddlschema).csv(div)

    ddl_df.show()

    print(ddl_df.printSchema())
    println(s"Total Rows = ${ddl_df.count()}")

    spark.stop()
  }
}