package com.soul.customdatasource.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Reader extends App {
  
  println("Reader Application Started")
  
  val conf = new SparkConf().setAppName("ReaderApp")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
  val df = spark.sqlContext.read.format("com.soul.customdatasource").load("data/")
  df.printSchema()
  //df.show()
  
  println("Rader Application Ended ")
}