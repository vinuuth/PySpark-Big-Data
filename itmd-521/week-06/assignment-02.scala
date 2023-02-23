package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object assignment02 {
    def main(args: Array[String]) {

        val spark = SparkSession.builder.appName("firescala").getOrCreate()
          
        if (args.length <= 0){
            println("usage Divvy_Trips </home/vagrant/mgowda2/itmd-521/labs/week-05/Divvy_Trips_2015-Q1.csv>")
            System.exit(1)
        }
    
        //Infer the Schema
        val csvFile=args(0)
        val inferDataFrame = spark.read.format("csv").option("header","true").option("inferSchema","true").load(csvFile)
        println("**************************************")
        inferDataFrame.show(false)
        println("***************printSchema inferred in Scala**************")
        println(inferDataFrame.printSchema)
        println("**************Number of records*****************")
        println("Number of records in infer DataFrame = "+ inferDataFrame.count())
        println("****************End of infer schema***************")
        
        //Defining schema programmatically 
        val schema = StructType(Array(StructField("device_id",IntegerType,false),
                        StructField("device_name",StringType,false),
                        StructField("ip",StringType,false),
                        StructField("cca2",StringType,false), 
                        StructField("cca3",StringType,false),
                        StructField("cn",StringType,false),
                        StructField("latitude",LongType,false),
                        StructField("longitude",LongType,false),
                        StructField("scale",StringType,false),
                        StructField("temp",IntegerType,false),
                        StructField("humidity",IntegerType,false),
                        StructField("battery_level",IntegerType,false),
                        StructField("c02_level",IntegerType,false),
                        StructField("lcd",StringType,false),
                        StructField("timestamp",LongType,false)
                        ))
        val structDataFrame = spark.read.schema(schema).csv(csvFile)  
        println("**************************************")
        structDataFrame.show(false)
        println("************printSchema programmatically in Scala*************")
        println(structDataFrame.printSchema)
    }
}