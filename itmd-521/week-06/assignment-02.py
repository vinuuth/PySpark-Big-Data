from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import *


if __name__ == "__main__":

    spark = (SparkSession
        .builder
        .appName("SF_Fire")
        .getOrCreate())
    # get the data set file name

    div = "Divvy_Trips_2015-Q1.csv"
    sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
    # read the file into a Spark DataFrame
    fire_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(sf_fire_file))
    fire_df.show()

    print(fire_df.printSchema())

    print("Total rows = %d" % (fire_df.count()))

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),      
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('Zipcode', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)])



fire_df =spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

fire_df.show()

print(fire_df.printSchema())
print(fire_df.schema)
print ("Total rows = %d" % (fire_df.count()))

