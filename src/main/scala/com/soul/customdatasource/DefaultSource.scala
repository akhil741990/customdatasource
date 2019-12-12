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
   
    val path = parameters.get("path")
    
    
    if (path!=null){
      new CustomDataSourceRelation(sqlContext,path.toString(),schema)
    }else{
      throw new IllegalArgumentException("Path must be passed for Custom DataSource")
    }
     
  }
  
}