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
f_df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))
f_df.show()
f_df.createOrReplaceTempView("us_delay_flights_tbl")



#Assignment part 1


# spark.sql("""SELECT distance, origin, destination
# FROM us_delay_flights_tbl WHERE distance > 1000
# ORDER BY distance DESC""").show(10)


#from pyspark.sql.functions import col, desc
(f_df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)


# spark.sql("""SELECT date, delay, origin, destination
# FROM us_delay_flights_tbl
# WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
# ORDER by delay DESC""").show(10)

#from pyspark.sql.functions import col

f_df = spark.table("us_delay_flights_tbl") \
    .select("date", "delay", "origin", "destination") \
    .where((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")) \
    .orderBy(col("delay").desc()) \
    .limit(10)

f_df.show()

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

#from pyspark.sql.functions import col, when

f_df = spark.table("us_delay_flights_tbl") \
    .select("delay", "origin", "destination", 
            when(col("delay") > 360, "Very Long Delays")
            .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
            .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
            .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
            .when(col("delay") == 0, "No Delays")
            .otherwise("Early").alias("Flight_Delays")) \
            .orderBy("origin", col("delay").desc())

f_df.show(10)




#Assignment part 2
# From page 90-92, you will create a Table named us_delay_flights_tbl from the departuredelay.csv
# Create a tempView of all flights with an origin of Chicago (ORD) and a month/day combo of between 03/01 and 03/15
# Show the first 5 records of the tempView, taking a screenshot
# Use the Spark Catalog to list the columns of table us_delay_flights_tbl

schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
fly_df = spark.read.csv(csv_file, schema=schema)
fly_df = fly_df.withColumn("dateMonth", from_unixtime(unix_timestamp(fly_df.date, "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(fly_df.date, "MMddHHmm"), "dd"))


# csv_file = "../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Schema as defined in the preceding example

fly_df.write.saveAsTable("us_delay_flights_tbl")

query= spark.sql(""" SELECT dateMonth, dateDay, delay, origin, destination
        FROM us_delay_flights_tbl
        WHERE origin ='ORD' AND dateMonth = 3 AND dateDay >1 AND dateDay <=15
        ORDER BY delay DESC
        LIMIT 5;
        """)
q_df = spark.read.query


print("From 1st to 15th March highest delays in ORD")


q_df.createOrReplaceTempView("us_delay_flights_tb1_tmp_view")
spark.sql("SELECT * FROM us_delay_flights_tbl_tmp_view").show(6)

print(spark.catlog.listTables())



# df_sfo = spark.sql("SELECT date, delay, origin, destination FROM
#  us_delay_flights_tbl WHERE origin = 'ORD'")
# df_jfk = spark.sql("SELECT date, delay, origin, destination FROM
#  us_delay_flights_tbl WHERE date = 'JFK'")





# Load the CSV file into a DataFrame
us_delay_flights_tbl = spark.read.format("csv").option("header", "true").schema(schema).load("../../../../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")

#from pyspark.sql.functions import col

# Create tempView


#us_delay_flights_tbl.createOrReplaceTempView("tempView")




#print("The number of flights between dates", chicago_flights)
# Show first 5 records
#chicago_flights.show(5)

#spark.catalog.listColumns("us_delay_flights_tbl")






