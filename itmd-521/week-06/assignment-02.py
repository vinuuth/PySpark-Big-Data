from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import *


if __name__ == "__main__":
    if len(sys.argv) <= 0:
        sys.exit(1)

    spark = (SparkSession
        .builder
        .appName("SF_Fire")
        .getOrCreate())
    sf_fire_file = sys.argv[1]
    # get the data set file name

    
    # read the file into a Spark DataFrame

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


# What were all the different types of fire calls in 2018?

fire_df.select("CallType").where((col("CallType").isNotNull()) & (col("CallDate").like("%2018%"))).distinct().show()


#What months within the year 2018 saw the highest number of fire calls?

fire_ts_df = (fire_df
              .withColumn("IDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))#.drop("CallDate") 
              .withColumn("OnDate",   to_timestamp(col("WatchDate"), "MM/dd/yyyy"))#.drop("WatchDate")
              .withColumn("AvlDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")))


fire_df2018 = fire_ts_df.select("IDate","CallTypeGroup","CallType","CallDate","City","Neighborhood","Delay").where((col("CallType").isNotNull()) & (col("CallDate").like("%2018%")))

max_count_df = fire_df2018.select(month("IDate").alias("month")).where((col("CallTypeGroup").isNotNull()) & (col("CallTypeGroup").like("Fire%"))).groupBy("month").agg(count("*").alias("count")).show()

max_count_df.select(*).orderBy(desc("count"))


#Which neighborhood in San Francisco generated the most fire calls in 2018?

fire_san=fire_df2018.select("Neighborhood").where((col("City").like("%San Francisco%")) & (col("CallTypeGroup").like("Fire%"))).groupBy("Neighborhood").count()
fire_san.show()

# Which neighborhoods had the worst response times to fire calls in 2018?

fire_delay=fire_df2018.select("Neighborhood","Delay").where(col("CallTypeGroup").like("Fire%")).orderBy(desc("Delay"))
fire_delay.show()

# Which week in the year in 2018 had the most fire calls?
fire_calls_2018_df = fire_ts_df.filter(fire_ts_df.year("IDate") == 2018)
fire_calls_week_df = fire_calls_2018_df.withColumn("week_number", fire_ts_df.weekofyear("date"))
fire_calls_count_df = fire_calls_week_df.groupBy("week_number").count()
fire_calls_count_sorted_df = fire_calls_count_df.sort(fire_ts_df.desc("count"))
most_fire_calls_week_2018 = fire_calls_count_sorted_df.first()["week_number"]


# Is there a correlation between neighborhood, zip code, and number of fire calls?

fire_calls_df = fire_df.filter(fire_df.CallTypeGroup.like("Fire%")).groupBy("Neighborhood", "Zipcode").agg(count("*").alias("num_calls"))
correlation = fire_calls_df.stat.corr("num_calls", "Zipcode")
fire_calls_df.show()
print("The correlation between number of fire calls and zip code is:", correlation)

# How can we use Parquet files or SQL tables to store this data and read it back?
