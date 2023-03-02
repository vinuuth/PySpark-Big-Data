from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == "__main__":

 spark = (SparkSession
 .builder
 .appName("Flightdelay")
 .getOrCreate())


# Path to data set

csv_file = "../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

#fly_schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
fy_schema= StructType([StructField('date', StringType(), True),
                     StructField('delay', StringType(), True),
                     StructField('distance', IntegerType(), True),
                     StructField('origin', StringType(), True),                  
                     StructField('destination', StringType(), True)])     
                     
fly_df = spark.read.csv(csv_file, header = True, schema = fy_schema)
fly_df.show()



#Assignment part 1

(fly_df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)




(fly_df.select("date", "delay", "origin", "destination") \
    .where((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")) \
    .orderBy(col("delay").desc()) \
    .limit(10)).show()




(fly_df.select("delay", "origin", "destination", 
            when(col("delay") > 360, "Very Long Delays")
            .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
            .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
            .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
            .when(col("delay") == 0, "No Delays")
            .otherwise("Early").alias("Flight_Delays")) \
            .orderBy("origin", col("delay").desc())).show(10)

#Assignment part 2


# From page 90-92, you will create a Table named us_delay_flights_tbl from the departuredelay.csv
# Create a tempView of all flights with an origin of Chicago (ORD) and a month/day combo of between 03/01 and 03/15
# Show the first 5 records of the tempView, taking a screenshot
# Use the Spark Catalog to list the columns of table us_delay_flights_tbl

#schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"

fly_df = fly_df.withColumn("dateMonth", from_unixtime(unix_timestamp(fly_df.date, "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(fly_df.date, "MMddHHmm"), "dd"))

# Schema as defined in the preceding example

#fly_df.write.saveAsTable("us_delay_flights_tbl")

fly_df.write.option("path","/home/vagrant//vbengaluruprabhudev/itmd-521/labs/week-07/spark-warehouse").mode("overwrite").saveAsTable("us_delay_flights_tbl")
query= "SELECT dateMonth, dateDay, delay, origin, destination FROM us_delay_flights_tbl WHERE origin ='ORD' AND dateMonth = 3 AND dateDay >= 1 AND dateDay <= 15 ORDER BY delay DESC LIMIT 5;"
sol_query_df= spark.sql(query)

sol_query_df.createOrReplaceTempView("us_delay_flights_tbl_tmp_view")
spark.sql("SELECT * FROM us_delay_flights_tbl_tmp_view").show()


#Assignment part 3

json_path="./spark-warehouse/json_path"
fly_df.write.format("json").mode("overwrite").option("compression", "none").json(json_path)


snappy_path = "./spark-warehouse/snappy_path"
fly_df.write.format("json").mode("overwrite").option("compression", "lz4").save(snappy_path)


parquet_path ="./spark-warehouse/parquet_path"
fly_df.write.format("parquet").mode("overwrite").parquet(parquet_path)


# Assignment part 4

# Using the departuredelays parquet file you created part III, read the content into a DataFrame, select all records that have ORD (Chicago O'Hare as Origin) and write the results to a DataFrameWriter named orddeparturedelays

# Use a .show(10) function to print out the first 10 lines, and take a screenshot
# Save as type Parquet
from pyspark.sql.functions import col
partfour_df= spark.read.parquet(parquet_path)
ord_df = partfour_df.select("*").where(col('origin') == 'ORD')
#ord_df.show(10)

partfour_parquet_path="./spark-warehouse/partfour_parquet_path"

orddeparturedelays = partfour_df.write.mode("overwrite")

orddeparturedelays.parquet(partfour_parquet_path)

ord_dep_df= spark.read.parquet(partfour_parquet_path)
ord_dep_df.show(10)


