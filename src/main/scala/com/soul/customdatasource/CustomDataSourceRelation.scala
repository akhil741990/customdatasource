package com.soul.customdatasource

import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.rdd.RDD
import com.soul.customdatasource.app.Util

class CustomDataSourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType) 
      extends BaseRelation with TableScan with Serializable {
  
  override def schema :  StructType = {
    if (userSchema != null) {
      userSchema
    } else {
     StructType(
        StructField("id", IntegerType, false) ::
        StructField("name", StringType, true) ::
        StructField("gender", StringType, true) ::
        StructField("salary", LongType, true) ::
        StructField("expenses", LongType, true) :: Nil
      )
    }
  }
  
  override def buildScan : RDD[Row] = {
    
    val schemaFields = schema.fields
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)
    
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) =>
          val colName = schemaFields(index).name
          Util.castTo(if (colName.equalsIgnoreCase("gender")) {if(value.toInt == 1) "Male" else "Female"} else value,
            schemaFields(index).dataType)
      })

      tmp.map(s => Row.fromSeq(s))
    })

    rows.flatMap(e => e)

  }
  
}