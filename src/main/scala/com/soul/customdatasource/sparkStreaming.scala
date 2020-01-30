val sourceTopic = "t_stream_sixteen_abc"
val broker = "172.31.3.48:9092"
val data = spark.readStream.format("kafka").option("kafka.bootstrap.servers",broker).option("subscribe",sourceTopic).option("startingOffsets","latest").load()



case class LineItemData(orderkey: Double, partkey: Double, supplierkey: Double, linenumber: Double, quantity: Double, extendedprice: Double, discount: Double, tax: Double, returnflag: String, status: String, shipdate: String, commitdate: String, receiptdate: String, shipinstructions: String, shipmode: String, comment: String)


import org.apache.spark.sql.Encoders
val schema = Encoders.product[LineItemData].schema


val rawValues = data.selectExpr("CAST(value AS STRING)").as[String]
val jsonValues = rawValues.select(from_json($"value", schema) as "record")
val liData = jsonValues.select("record.*").as[LineItemData]

val query = liData.writeStream.queryName("temp").outputMode("append").format("memory").start()
