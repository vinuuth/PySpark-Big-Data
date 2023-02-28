from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = (SparkSession
 .builder
 .appName("SparkSQLExampleApp")
 .getOrCreate())


# Path to data set
S
csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")