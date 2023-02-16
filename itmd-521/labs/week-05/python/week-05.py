from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('csv-reader').getOrCreate()

# Infer the schema and read the CSV file
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("../Divvy_Trips_2015-Q1.csv")

# Print the schema
df.printSchema()

# Show the data
df.show()
