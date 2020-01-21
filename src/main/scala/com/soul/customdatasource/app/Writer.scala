package com.soul.customdatasource.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

class Writer {
  
  
  val conf = new SparkConf().setAppName("ReaderApp")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
  val df = spark.sqlContext.read.format("com.soul.customdatasource").load("myData/")
  var options = Map(("format","customFormat"), ("path","home/akhil/devTools/soul/spark/spark-2.4.4-bin-hadoop2.7/outputData/")) 
  df.write.options(options).mode(SaveMode.Overwrite).format("com.soul.customdatasource").save("out_data")
}