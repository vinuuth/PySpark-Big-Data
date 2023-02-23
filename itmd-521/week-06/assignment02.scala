package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment02 {
    def main(args: Array[String]) {

        val spark = SparkSession.builder.appName("iotscala").getOrCreate()
          
        if (args.length <= 0){
            System.exit(1)
        }
        val fileJson=args(0)
        case class IoTData (battery_level: Long, c02_level: Long,cca2: String, cca3: String, cn: String, device_id: Long,device_name: String, humidity: Long, ip: String, latitude: Double,lcd: String, longitude: Double, scale:String, temp: Long,timestamp: Long)

        // read the file into a Spark DataFrame
        val device_df = spark.read.format("json").option("header", "true").option("inferSchema", "true").load(fileJson)
        println(device_df.printSchema)


  //Detect failing devices with battery levels below a threshold.
  val threshold= device_df.select("device_name","battery_level").where("battery_level<4")
        threshold.show()
    }
}