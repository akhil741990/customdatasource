package com.soul.customdatasource

import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.hadoop.fs.Path

class DefaultSource extends RelationProvider with SchemaRelationProvider  with CreatableRelationProvider {
  
  
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
  
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", "./myOutputData/") 
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    mode match {
      case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists => sys.error("Given path: " + path + " exists!!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }

    val formatName = parameters.getOrElse("format", "customFormat")
    formatName match {
      case "customFormat" => saveAsCustomFormat(data, path, mode)
      case "json" => saveAsJson(data, path, mode)
      case _ => throw new IllegalArgumentException(formatName + " is not supported!!!")
    }
    createRelation(sqlContext, parameters, data.schema)
  }
  
  private def saveAsJson(data : DataFrame, path : String, mode: SaveMode): Unit = {
    data.write.mode(mode).json(path)
  }

  private def saveAsCustomFormat(data : DataFrame, path : String, mode: SaveMode): Unit = {
    
    val customFormatRDD = data.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString(",")
    })
    customFormatRDD.saveAsTextFile(path)
  }  
  
}