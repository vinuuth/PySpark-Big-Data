package main.scala.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment03 {
    def main(args: Array[String]) {

val spark = (SparkSession
.builder
.appName("Flightdelay")
.getOrCreate())

    if (args.length < 1) {
      print("Usage: flights <csv_file_dataset>")
      sys.exit(1)
    }

// Path to data set
val csv_file = args(0)
// val csv_file = "../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

// Schema for flights data
val fy_schema = StructType(Array(
StructField("date", StringType, true),
StructField("delay", IntegerType, true),
StructField("distance", IntegerType, true),
StructField("origin", StringType, true),
StructField("destination", StringType, true)
))

val fli_df = spark.read.schema(fy_schema).csv(csv_file)

fli_df.show()


//Assignment part 1
//For the first query
fli_df.select("distance", "origin", "destination")
 .filter(col("distance") > (1000))
 .orderBy(desc("distance"))
 .show(10)

// For the second query
fli_df.select("date", "delay", "origin", "destination")
 .filter(col("delay") > (120) && col("origin") === "SFO" && col("destination") === "ORD")
 .orderBy(desc("delay"))
 .show(10)

//For the third query
fli_df.select(col("delay"), col("origin"), col("destination"),
    when(col("delay") > (360), "Very Long Delays")
    .when(col("delay") > (120) && col("delay") < (360), "Long Delays")
    .when(col("delay") > (60) && col("delay") < (120), "Short Delays")
    .when(col("delay") > (0) && col("delay") < (60), "Tolerable Delays")
    .when(col("delay") === 0, "No Delays")
    .otherwise("Early")
    .alias("Flight_Delays"))
    .orderBy(col("origin"), desc("delay"))
    .show(10)


// Assignment part 2

import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
val fly_date_df = fli_df.withColumn("dateMonth", from_unixtime(unix_timestamp(col("date"), "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(col("date"), "MMddHHmm"), "dd"))

fly_date_df.write.option("path","/home/vagrant//vbengaluruprabhudev/itmd-521/labs/week-07/spark-warehouse").mode("overwrite").saveAsTable("us_delay_flights_tbl")

val query= "SELECT dateMonth, dateDay, delay, origin, destination FROM us_delay_flights_tbl WHERE origin ='ORD' AND dateMonth = 3 AND dateDay >= 1 AND dateDay <= 15 ORDER BY delay DESC LIMIT 5;"
val sol_query_df = spark.sql(query)

sol_query_df.createOrReplaceTempView("us_delay_flights_tbl_tmp_view")
spark.sql("SELECT * FROM us_delay_flights_tbl_tmp_view").show()



//Assignment part 3

val json_path="./spark-warehouse/json_path"
fli_df.write.format("json").mode("overwrite").option("compression", "none").json(json_path)


val snappy_path = "./spark-warehouse/snappy_path"
fli_df.write.format("json").mode("overwrite").option("compression", "lz4").save(snappy_path)


val parquett_path ="./spark-warehouse/parquet_path"
fli_df.write.format("parquet").mode("overwrite").parquet(parquett_path)


//Assignment part 4

val fl_df = spark.read.parquet(parquett_path)
val flight_ord_df = fl_df.select("*").where(col("origin") === "ORD")
val ord_parquet_path = "./spark-warehouse/new_parquet_path"
val orddeparturedelays = flight_ord_df.write.mode("overwrite")
orddeparturedelays.parquet(ord_parquet_path)
val dep_df = spark.read.parquet(ord_parquet_path)
dep_df.show(10)









    }
}

