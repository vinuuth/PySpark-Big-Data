from __future__ import print_function

import sys

from pyspark.sql import SparkSession

spark = (SparkSession
      .builder
      .appName("JRH - Mysql Cluster Example")
      .getOrCreate())

# Password is availabe under Blackboard > Assignments > Week-12

jdbcDF = (spark.read.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","employees").option("user","worker").option("password", "").load())

jdbcDF.show(10);