package org.apache.spark.sql

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.execution.datasources.hbase.{AvroSedes, HBaseTableCatalog}
import org.apache.spark.{Logging, SparkContext}

case class AvroHBaseRecord(col0: String,
                           col1: Array[Byte])

object AvroHBaseRecord {
  val schemaString =
    s"""{"namespace": "example.avro",
         |   "type": "record",      "name": "User",
         |    "fields": [
         |        {"name": "name", "type": "string"},
         |        {"name": "favorite_number",  "type": ["int", "null"]},
         |        {"name": "favorite_color", "type": ["string", "null"]},
         |        {"name": "favorite_array", "type": {"type": "array", "items": "string"}},
         |        {"name": "favorite_map", "type": {"type": "map", "values": "int"}}
         |      ]    }""".stripMargin

  val avroSchema: Schema = {
    val p = new Schema.Parser
    p.parse(schemaString)
  }

  def apply(i: Int): AvroHBaseRecord = {

    val user = new GenericData.Record(avroSchema);
    user.put("name", s"name${"%03d".format(i)}")
    user.put("favorite_number", i)
    user.put("favorite_color", s"color${"%03d".format(i)}")
    val favoriteArray = new GenericData.Array[String](2, avroSchema.getField("favorite_array").schema())
    favoriteArray.add(s"number${i}")
    favoriteArray.add(s"number${i+1}")
    user.put("favorite_array", favoriteArray)
    import collection.JavaConverters._
    val favoriteMap = Map[String, Int](("key1" -> i), ("key2" -> (i+1))).asJava
    user.put("favorite_map", favoriteMap)
    val avroByte = AvroSedes.serialize(user, avroSchema)
    AvroHBaseRecord(s"name${"%03d".format(i)}", avroByte)
  }
}
class AvroSourceSuite extends SHC with Logging{
  override def catalog = s"""{
            |"table":{"namespace":"default", "name":"avrotable"},
            |"rowkey":"key",
            |"columns":{
              |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
              |"col1":{"cf":"cf1", "col":"col1", "type":"binary"}
            |}
          |}""".stripMargin

  def avroCatalog = s"""{
            |"table":{"namespace":"default", "name":"avrotable"},
            |"rowkey":"key",
            |"columns":{
              |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
              |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
            |}
          |}""".stripMargin

  def avroCatalogInsert = s"""{
            |"table":{"namespace":"default", "name":"avrotableInsert"},
            |"rowkey":"key",
            |"columns":{
              |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
              |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
            |}
          |}""".stripMargin


  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map("avroSchema"->AvroHBaseRecord.schemaString, HBaseTableCatalog.tableCatalog->avroCatalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("populate table") {
    //createTable(tableName, columnFamilies)
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      AvroHBaseRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("empty column") {
    val df = withCatalog(catalog)
    df.registerTempTable("avrotable")
    val c = sqlContext.sql("select count(1) from avrotable").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("full query") {
    val df = withCatalog(catalog)
    df.show
    df.printSchema()
    assert(df.count() == 256)
  }

  test("array field") {
    val df = withCatalog(avroCatalog)
    val filtered = df.select($"col0", $"col1.favorite_array").where($"col0" === "name001")
    assert(filtered.count() == 1)
    val collected = filtered.collect()
    assert(collected(0).getSeq[String](1)(0) === "number1")
    assert(collected(0).getSeq[String](1)(1) === "number2")
  }

  test("map field") {
    val df = withCatalog(avroCatalog)
    val filtered = df.select(
        $"col0",
        $"col1.favorite_map".getItem("key1").as("key1"),
        $"col1.favorite_map".getItem("key2").as("key2")
      )
      .where($"col0" === "name001")
    assert(filtered.count() == 1)
    val collected = filtered.collect()
    assert(collected(0)(1) === 1)
    assert(collected(0)(2) === 2)
  }

  test("serialization and deserialization query") {
    val df = withCatalog(catalog)
    df.write.options(
      Map("avroSchema"->AvroHBaseRecord.schemaString, HBaseTableCatalog.tableCatalog->avroCatalogInsert,
        HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    val newDF = withCatalog(avroCatalogInsert)
    newDF.show
    newDF.printSchema()
    assert(newDF.count() == 256)
  }

  test("filtered query") {
    val df = withCatalog(catalog)
    val r = df.filter($"col1.name" === "name005" || $"col1.name" <= "name005").select("col0", "col1.favorite_color", "col1.favorite_number")
    r.show
    assert(r.count() == 6)
  }

  test("Or filter") {
    val df = withCatalog(catalog)
    val s = df.filter($"col1.name" <= "name005" || $"col1.name".contains("name007"))
      .select("col0", "col1.favorite_color", "col1.favorite_number")
    s.show
    assert(s.count() == 7)
  }

}
