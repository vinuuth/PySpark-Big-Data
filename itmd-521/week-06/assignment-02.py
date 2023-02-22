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
#df = df.withColumn("CallDate", to_date("date_column", "yyyy-MM-dd"))
fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().count().show()
#fire_df.select("CallType","CallDate").where(col("CallType").isNotNull()).show()
#fire_df.select("CallType").where((col("CallType").isNotNull()) & (year("CallDate").like("2018%"))).show()
#fire_df.select("CallType",year("date_column").alias("year")).where(col("CallType").isNotNull()).distinct().show(10)
# .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 #.show())

#sf_fire_file.filter(year('CallDate')=='2018')

#select("CallType").where(col("CallType").isNotNull()).show()

fire_df.select("CallType").where(col("CallType").isNotNull()).show()
