package com.soul.customdatasource

import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.types._

class CustomDataSourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType) 
      extends BaseRelation with Serializable {
  
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
  
}