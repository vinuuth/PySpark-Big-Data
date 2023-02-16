package main.scala.chapter3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object week05 {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("Divyscala")
        .getOrCreate()
    val div = "Divvy_Trips_2015-Q1.csv"

    val div_DF = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(div))
    div_DF.show(false)

    print(div_DF.printSchema())

    println(s"Total Rows = ${div_DF.count()}")
    println()


   val divschema =  StructType(Array(StructField("trip_id", IntegerType, true),
                 StructField("starttime", StringType, true),
                 StructField("stoptime", StringType(), true),
                 StructField("bikeid", IntegerType(), true),
                 StructField("tripduration", IntegerType(), true),
                 StructField("from_station_id", IntegerType(), true),
                 StructField("from_staion_name", StringType(), true),
                 StructField("to_station_id", IntegerType(), true),
                 StructField("to_station_name", StringType(), true),
                 StructField("usertype", StringType(), true),
                 StructField("gender", StringType(), true),
                 StructField("birthyear", IntegerType(), true)))

    val divy_DF = spark.read.schema(divschema).csv(div)

    divy_DF.show(false)


    print(divy_DF.printSchema())
    print(divy_DF.schema)
    println(s"Total Rows = ${divy_DF.count()}")



   val ddlschema = "trip_id INT, stattime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

   val ddl_DF = spark.read.schema(ddlschema).csv(div)

    ddl_DF.show(false)

    print(ddl_DF.printSchema())
    println(s"Total Rows = ${ddl_DF.count()}")
    println()


    spark.stop()
  }
}