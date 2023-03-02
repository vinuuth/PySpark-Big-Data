package main.scala.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Flightdelay {
    def main(args: Array[String]) {

val spark = (SparkSession
.builder
.appName("Flightdelay")
.getOrCreate())

// Path to data set
val csv_file = "../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

// Schema for flights data
val fy_schema = StructType(Array(
StructField("date", StringType, true),
StructField("delay", StringType, true),
StructField("distance", IntegerType, true),
StructField("origin", StringType, true),
StructField("destination", StringType, true)
))

val fly_df = spark.read.schema(fy_schema).csv(csv_file)

fly_df.show()

//For the first query
fly_df.select("distance", "origin", "destination")
.where(col("distance") > 1000)
.orderBy(desc("distance"))
.show(10)

// For the second query
fly_df.select("date", "delay", "origin", "destination")
.where((col("delay") > 120) && (col("origin") === "SFO") && (col("destination") === "ORD"))
.orderBy(col("delay").desc())
.limit(10)
.show()

// For the third query
fly_df.select("delay", "origin", "destination",
when(col("delay") > 360, "Very Long Delays")
.when((col("delay") > 120) && (col("delay") < 360), "Long Delays")
.when((col("delay") > 60) && (col("delay") < 120), "Short Delays")
.when((col("delay") > 0) && (col("delay") < 60), "Tolerable Delays")
.when(col("delay") === 0, "No Delays")
.otherwise("Early")
.alias("Flight_Delays"))
.orderBy("origin", col("delay").desc())
.show(10)

    }
}

