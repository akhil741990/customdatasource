package com.soul.customdatasource

import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.BaseRelation

class DefaultSource extends RelationProvider with SchemaRelationProvider{
  
  
  override def createRelation(sqlContext: SQLContext, parameters: Map[String,String]) :  BaseRelation = {
   
    createRelation(sqlContext, parameters, null)  
  }
  
  override def createRelation(sqlContext: SQLContext, parameters: Map[String,String], schema : StructType) :  BaseRelation = {
   
    val path = show(parameters.get("path"))
    
    
    println(path)
    
    if (path!=null){
      //new CustomDataSourceRelation(sqlContext,"/home/akhil/devTools/soul/custom-datasource/customdatasource/data/",schema)
      new CustomDataSourceRelation(sqlContext,path,schema)
    }else{
      throw new IllegalArgumentException("Path must be passed for Custom DataSource")
    }
     
  }
  
  def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
   }
  
}