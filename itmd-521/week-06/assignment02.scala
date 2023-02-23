package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object assignment02 {
    def main(args: Array[String]) {

        val spark = SparkSession.builder.appName("firescala").getOrCreate()
          
        if (args.length <= 0){
            System.exit(1)
        }
    
        val csvFile=args(0)
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
        println("************printSchema programmatically in Scala*************")
        println(structDataFrame.printSchema)
    }
}