package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment02 {
    def main(args: Array[String]) {

        val spark = SparkSession.builder.appName("Iotscala").getOrCreate()
          
        if (args.length <= 0){
            System.exit(1)
        }
        val fileJson=args(0)
        case class IoTData (battery_level: Long, c02_level: Long,cca2: String, cca3: String, cn: String, device_id: Long,device_name: String, humidity: Long, ip: String, latitude: Double,lcd: String, longitude: Double, scale:String, temp: Long,timestamp: Long)

        // read the file into a Spark DataFrame
        val device_df = spark.read.format("json").option("header", "true").option("inferSchema", "true").load(fileJson)
        println(device_df.printSchema)


  //1)Detect failing devices with battery levels below a threshold.
  val threshold= device_df.select("device_name","battery_level").where("battery_level<3")
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


 //2)Identify offending countries with high levels of CO2 emissions.
    val emission= device_df.groupBy("cn").agg(avg("c02_level").alias("av_co2_level")).orderBy("av_co2_level").sort(desc("av_co2_level"))
    emission.show()

// +----------------+------------------+
// |              cn|      av_co2_level|
// +----------------+------------------+
// |           Gabon|            1523.0|
// |Falkland Islands|            1424.0|
// |          Monaco|            1421.5|
// |          Kosovo|            1389.0|
// |      San Marino|1379.6666666666667|
// |         Liberia|            1374.5|
// |           Syria|            1345.8|
// |      Mauritania|1344.4285714285713|
// |           Congo|          1333.375|
// |           Tonga|            1323.0|
// |      East Timor|            1310.0|
// |          Guinea|            1308.0|
// |        Botswana|1302.6666666666667|
// |           Haiti|1291.3333333333333|
// |            Laos|            1291.0|
// |        Maldives|1284.7272727272727|
// |    Sint Maarten|1282.2857142857142|
// |         Andorra|            1279.0|
// |         Lesotho|            1274.6|
// |      Mozambique|            1264.0|
// +----------------+------------------+
//only showing top 20 rows
        
 //3)Compute the min and max values for temperature, battery level, CO2, and humidity.
        val optvalue = device_df.select(min("temp").alias("min_temperature"),
        max("temp").alias("max_temperature"),
        min("battery_level").alias("min_batteryLevel"),
        max("battery_level").alias("max_batteryLevel"),
        min("c02_level").alias("min_co2"),
        max("c02_level").alias("max_co2"),
        min("humidity").alias("min_humidity"),
        max("humidity").alias("max_humidity"))
        optvalue.show()

// +---------------+---------------+----------------+----------------+-------+-------+------------+------------+
// |min_temperature|max_temperature|min_batteryLevel|max_batteryLevel|min_co2|max_co2|min_humidity|max_humidity|
// +---------------+---------------+----------------+----------------+-------+-------+------------+------------+
// |             10|             34|               0|               9|    800|   1599|          25|          99|
// +---------------+---------------+----------------+----------------+-------+-------+------------+------------+



 //4)Sort and group by average temperature, CO2, humidity, and country.
        val grpavg = device_df.groupBy("cn").agg(avg("temp").alias("av_temperapure"),avg("c02_level").alias("av_co2_level"),
        avg("humidity").alias("av_humidity")).orderBy("av_temperapure", "av_co2_level", "av_humidity").sort(desc("av_temperapure"))
        grpavg.show()

// +--------------------+------------------+------------------+------------------+
// |                  cn|    av_temperapure|      av_co2_level|       av_humidity|
// +--------------------+------------------+------------------+------------------+
// |            Anguilla|31.142857142857142| 1165.142857142857|50.714285714285715|
// |           Greenland|              29.5|            1099.5|              56.5|
// |               Gabon|              28.0|            1523.0|              30.0|
// |             Vanuatu|              27.3|            1175.3|              64.0|
// |         Saint Lucia|              27.0|1201.6666666666667|61.833333333333336|
// |        Turkmenistan|26.666666666666668|            1093.0|              69.0|
// |              Malawi|26.666666666666668|            1137.0| 59.55555555555556|
// |                Iraq|26.428571428571427|1225.5714285714287| 62.42857142857143|
// |                Laos|26.285714285714285|            1291.0|60.857142857142854|
// |British Indian Oc...|              26.0|            1206.0|              65.0|
// |                Cuba|25.866666666666667|1222.5333333333333| 49.53333333333333|
// |               Haiti|25.333333333333332|1291.3333333333333| 64.58333333333333|
// |                Fiji| 25.09090909090909|1193.7272727272727| 56.45454545454545|
// |            Dominica| 24.73076923076923|1214.3461538461538| 70.46153846153847|
// |               Benin|24.666666666666668|            1038.0| 65.66666666666667|
// |               Syria|              24.6|            1345.8|              57.8|
// |            Botswana|              24.5|1302.6666666666667|             73.75|
// |Northern Mariana ...|24.333333333333332| 1164.111111111111|52.333333333333336|
// |          East Timor|24.333333333333332|            1310.0|              59.0|
// |             Bahamas| 24.27777777777778| 1177.388888888889| 68.61111111111111|
// +--------------------+------------------+------------------+------------------+


    }
}