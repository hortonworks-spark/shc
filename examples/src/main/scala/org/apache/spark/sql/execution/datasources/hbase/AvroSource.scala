/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase.examples

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.execution.datasources.hbase.types.AvroSerde

case class AvroHBaseRecord(col0: String,
                           col1: Array[Byte])

object AvroHBaseRecord {
  val schemaString =
    s"""{"namespace": "example.avro",
        |   "type": "record", "name": "User",
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

    val user = new GenericData.Record(avroSchema)
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
    val avroByte = AvroSerde.serialize(user, avroSchema)
    AvroHBaseRecord(s"name${"%03d".format(i)}", avroByte)
  }
}

object AvroSource {
  def catalog = s"""{
                     |"table":{"namespace":"default", "name":"shcExampleAvrotable", "tableCoder":"PrimitiveType"},
                     |"rowkey":"key",
                     |"columns":{
                     |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"col1":{"cf":"cf1", "col":"col1", "type":"binary"}
                     |}
                     |}""".stripMargin

  def avroCatalog = s"""{
                        |"table":{"namespace":"default", "name":"shcExampleAvrotable", "tableCoder":"PrimitiveType"},
                        |"rowkey":"key",
                        |"columns":{
                        |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                        |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                        |}
                        |}""".stripMargin

  def avroCatalogInsert = s"""{
                              |"table":{"namespace":"default", "name":"shcExampleAvrotableInsert", "tableCoder":"PrimitiveType"},
                              |"rowkey":"key",
                              |"columns":{
                              |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                              |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                              |}
                              |}""".stripMargin

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("AvroExample")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map("avroSchema" -> AvroHBaseRecord.schemaString, HBaseTableCatalog.tableCatalog -> avroCatalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val data = (0 to 255).map { i =>
      AvroHBaseRecord(i)
    }

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    val df = withCatalog(catalog)
    df.show
    df.printSchema()
    df.createOrReplaceTempView("shcExampleAvrotable")
    val c = sqlContext.sql("select count(1) from shcExampleAvrotable")
    c.show

    val filtered = df.select($"col0", $"col1.favorite_array").where($"col0" === "name001")
    filtered.show
    val collected = filtered.collect()
    if (collected(0).getSeq[String](1)(0) != "number1") {
      throw new Exception("value invalid")
    }
    if (collected(0).getSeq[String](1)(1) != "number2") {
      throw new Exception("value invalid")
    }

    df.write.options(
      Map("avroSchema"->AvroHBaseRecord.schemaString, HBaseTableCatalog.tableCatalog->avroCatalogInsert,
        HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    val newDF = withCatalog(avroCatalogInsert)
    newDF.show
    newDF.printSchema()
    if(newDF.count() != 256) {
      throw new Exception("value invalid")
    }

    df.filter($"col1.name" === "name005" || $"col1.name" <= "name005")
      .select("col0", "col1.favorite_color", "col1.favorite_number")
      .show

    df.filter($"col1.name" <= "name005" || $"col1.name".contains("name007"))
      .select("col0", "col1.favorite_color", "col1.favorite_number")
      .show

    spark.stop()
  }
}
