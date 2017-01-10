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
 */

package org.apache.spark.sql

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.execution.datasources.hbase.types.AvroSerde

case class AvroCompositeKeyRecord(col0: String,
                                  col1: Int,
                                  col2: Array[Byte],
                                  col3: Array[Byte],
                                  col4: Array[Byte])

object AvroCompositeKeyRecord {
  val schemaString =
    s"""{"namespace": "example.avro",
        |   "type": "record", "name": "User",
        |    "fields": [ {"name": "name", "type": "string"},
        |      {"name": "favorite_number",  "type": ["int", "null"]},
        |        {"name": "favorite_color", "type": ["string", "null"]} ] }""".stripMargin

  val avroSchema: Schema = {
    val p = new Schema.Parser
    p.parse(schemaString)
  }

  def apply(i: Int): AvroCompositeKeyRecord = {
    val user = new GenericData.Record(avroSchema);
    user.put("name", s"name${"%03d".format(i)}")
    user.put("favorite_number", i)
    user.put("favorite_color", s"color${"%03d".format(i)}")
    val avroByte = AvroSerde.serialize(user, avroSchema)
    AvroCompositeKeyRecord(s"name${"%03d".format(i)}", i, avroByte, avroByte, avroByte)
  }
}

class AvroSourceCompositeKeySuite extends SHC with Logging {
  // 'catalog' is used when saving data to HBase
  override def catalog = s"""{
                             |"table":{"namespace":"default", "name":"avrotable"},
                             |"rowkey":"key1:key2:key3:key4",
                             |"columns":{
                             |"col0":{"cf":"rowkey", "col":"key1", "type":"string"},
                             |"col1":{"cf":"rowkey", "col":"key2", "type":"int"},
                             |"col2":{"cf":"rowkey", "col":"key3", "type":"binary"},
                             |"col3":{"cf":"rowkey", "col":"key4", "type":"binary"},
                             |"col4":{"cf":"cf1", "col":"col1", "type":"binary"}
                             |}
                             |}""".stripMargin

  def avroCatalog = s"""{
                        |"table":{"namespace":"default", "name":"avrotable"},
                        |"rowkey":"key1:key2:key3:key4",
                        |"columns":{
                        |"col0":{"cf":"rowkey", "col":"key1", "type":"string"},
                        |"col1":{"cf":"rowkey", "col":"key2", "type":"int"},
                        |"col2":{"cf":"rowkey", "col":"key3", "avro":"avroSchema"},
                        |"col3":{"cf":"rowkey", "col":"key4", "avro":"avroSchema"},
                        |"col4":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                        |}
                        |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map("avroSchema" -> AvroHBaseKeyRecord.schemaString, HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("populate table") {
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      AvroCompositeKeyRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("empty column") {
    val df = withCatalog(avroCatalog)
    df.registerTempTable("avrotable")
    val c = sqlContext.sql("select count(1) from avrotable").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

 test("full query") {
    val df = withCatalog(avroCatalog)
    df.show
    df.printSchema()
    assert(df.count() == 256)
  }

  test("filtered query") {
    val df = withCatalog(avroCatalog)
    val r = df.filter($"col2.name" === "name005" || $"col2.name" <= "name005")
      .select("col0", "col2.favorite_color", "col2.favorite_number")
    r.show
    assert(r.count() == 6)
  }

  test("Or filter") {
    val df = withCatalog(avroCatalog)
    val s = df.filter($"col2.name" <= "name005" || $"col3.name".contains("name007"))
      .select("col0", "col2.favorite_color", "col4.favorite_number")
    s.show
    assert(s.count() == 7)
  }
}