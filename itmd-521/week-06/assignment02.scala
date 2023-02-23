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
  val threshold= device_df.select("device_name","battery_level").where("battery_level<5")
        threshold.show()


// +--------------------+-------------+
// |         device_name|battery_level|
// +--------------------+-------------+
// | device-mac-36TWSKiT|            2|
// |sensor-pad-8xUD6p...|            0|
// |sensor-pad-12Y2kIm0o|            0|
// |sensor-pad-14QL93...|            1|
// |meter-gauge-17zb8...|            0|
// |sensor-pad-36VQv8...|            1|
// |device-mac-39iklY...|            2|
// | sensor-pad-40NjeMqS|            2|
// |meter-gauge-43RYo...|            2|
// | sensor-pad-448DeWGL|            0|
// | sensor-pad-52eFObBC|            2|
// |meter-gauge-77IKW...|            1|
// |sensor-pad-80TY4d...|            0|
// |sensor-pad-84jla9J5O|            1|
// | therm-stick-85NcuaO|            1|
// |device-mac-87EJxth2l|            1|
// | sensor-pad-92vxuq7e|            0|
// |sensor-pad-98mJQA...|            0|
// |device-mac-99Xh5Y...|            2|
// |sensor-pad-102D03...|            2|
// +--------------------+-------------+


 //2)Identify offending countries with high levels of CO2 emissions.
        val co2 = device_df.groupBy("cn").agg(avg("c02_level").alias("av_co2_lvl")).orderBy("av_co2_lvl").sort(desc("av_co2_lvl"))
        co2.show()
        


    }
}