from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = (SparkSession
 .builder
 .appName("SparkSQLExampleApp")
 .getOrCreate())


# Path to data set

csv_file = "../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))
df.show()
df.createOrReplaceTempView("us_delay_flights_tbl")




# spark.sql("""SELECT distance, origin, destination
# FROM us_delay_flights_tbl WHERE distance > 1000
# ORDER BY distance DESC""").show(10)


from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)


# spark.sql("""SELECT date, delay, origin, destination
# FROM us_delay_flights_tbl
# WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
# ORDER by delay DESC""").show(10)

from pyspark.sql.functions import col

df = spark.table("us_delay_flights_tbl") \
    .select("date", "delay", "origin", "destination") \
    .where((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")) \
    .orderBy(col("delay").desc()) \
    .limit(10)

df.show()

# spark.sql("""SELECT delay, origin, destination,
#  CASE
#  WHEN delay > 360 THEN 'Very Long Delays'
#  WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
#  WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
#  WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
#  WHEN delay = 0 THEN 'No Delays'
#  ELSE 'Early'
#  END AS Flight_Delays
#  FROM us_delay_flights_tbl
#  ORDER BY origin, delay DESC""").show(10)

from pyspark.sql.functions import col, when

df = spark.table("us_delay_flights_tbl") \
    .select("delay", "origin", "destination", 
            when(col("delay") > 360, "Very Long Delays")
            .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
            .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
            .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
            .when(col("delay") == 0, "No Delays")
            .otherwise("Early").alias("Flight_Delays")) \
            .orderBy("origin", col("delay").desc())

df.show(10)

# Path to our US flight delays CSV file
csv_file = "../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Schema as defined in the preceding example
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("us_delay_flights_tbl")

# df_sfo = spark.sql("SELECT date, delay, origin, destination FROM
#  us_delay_flights_tbl WHERE origin = 'ORD'")
# df_jfk = spark.sql("SELECT date, delay, origin, destination FROM
#  us_delay_flights_tbl WHERE date = 'JFK'")

from pyspark.sql.functions import col

# Create tempView
us_delay_flights_tbl.createOrReplaceTempView("tempView")

# Filter for flights with ORD origin and March 1-15 date range
chicago_flights = spark.sql("SELECT * FROM tempView WHERE origin = 'ORD' AND date >= '2008-03-01' AND date <= '2008-03-15'")

# Show first 5 records
chicago_flights.show(5)






