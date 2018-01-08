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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}

import scala.reflect.ClassTag

case class HBaseRecordExtended(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte,
                        col9: String)

object HBaseRecordExtended {
  def apply(i: Int, t: String): HBaseRecordExtended = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecordExtended(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i: $t",
      i.toByte,
      s"StringExtended$i: $t")
  }
  val catalog = s"""{
                   |"table":{"namespace":"default", "name":"table1"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                   |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                   |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                   |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                   |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                   |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                   |"col7":{"cf":"cf7", "col":"col7_1", "type":"string"},
                   |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"},
                   |"col9":{"cf":"cf7", "col":"col7_2", "type":"string"}
                   |}
                   |}""".stripMargin

  val catalogcf7 = s"""{
                   |"table":{"namespace":"default", "name":"table1"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"cf7", "col":"col7_1", "type":"string"},
                   |"col2":{"cf":"cf7", "col":"col7_2", "type":"string"}
                   |}
                   |}""".stripMargin

}

case class HBaseRecordDynamic(
                                col0: String,
                                col1: Boolean,
                                col2: Map[Long, Double],
                                col3: Float,
                                col4: Map[String, Int],
                                col5: Long,
                                col6: Short,
                                col7: Map[String, Map[Long, String]],
                                col8: Byte)

object HBaseRecordDynamic {
  def apply(i: Int, t: String): HBaseRecordDynamic = {
    val s = s"""row${"%03d".format(i)}"""
    val ts = System.currentTimeMillis() - i * 1000
    HBaseRecordDynamic(s,
      i % 2 == 0,
      Map(ts -> i.toDouble),
      i.toFloat,
      Map("col4" -> i),
      i.toLong,
      i.toShort,
      Map("col7" -> Map(ts -> s"String$i: $t")),
      i.toByte)
  }
  val catalog = s"""{
                   |"table":{"namespace":"default", "name":"table1"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                   |"col2":{"cf":"cf2", "col":"col2", "type":"map<long, double>"},
                   |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                   |"col4":{"cf":"cf4", "col":"", "type":"map<string, int>"},
                   |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                   |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                   |"col7":{"cf":"cf7", "col":"", "type":"map<string, map<long, string>>"},
                   |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                   |}
                   |}""".stripMargin

}


class DynamicColumnSuite extends SHC with Logging {

  def withCatalog(cat: String, opt: Map[String, String]): DataFrame = {
    sqlContext
      .read
      .options(Map(
        HBaseTableCatalog.tableCatalog -> cat
      ) ++ opt)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def writeData[T: ClassTag](data: Seq[T], catalog: String)(implicit newProductEncoder: Encoder[T], rddToDatasetHolder: RDD[T] => DatasetHolder[T]) = {
    sc.parallelize(data).toDF.write
      .options(Map(
        HBaseTableCatalog.tableCatalog -> catalog,
        HBaseTableCatalog.newTable -> "5",
        HBaseRelation.MAX_VERSIONS -> "3"
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("retrieve rows without schema with default type") {
    val sql = sqlContext
    import sql.implicits._

    sc.parallelize(data).toDF.write


    def data = (0 to 2).map { i =>
      HBaseRecordExtended(i, "schema less")
    }

    writeData(data, HBaseRecordExtended.catalog)
    writeData(data.map(_.copy(col1 = false, col2 = 1.1, col4 = 1, col7 = "")), HBaseRecordExtended.catalog)

    // Test

    val result = withCatalog(HBaseRecordDynamic.catalog, Map(
      HBaseRelation.RESTRICTIVE -> HBaseRelation.Restrictive.none,
      HBaseRelation.MAX_VERSIONS -> "3"
    ))


    val rows = result.take(10)


    assert(rows.size == 3)
    println(rows.mkString(" | "))
    assert(rows(0).size == 9)
    assert(rows(0).getBoolean(1) == false)
    assert(rows(0).getMap[Long, Double](2).size == 2)
    assert(rows(0).getMap[String, Int](4).head._2 == 1)
    assert(rows(0).getMap[String,Map[Long, String]](7).size == 2)

  }

  test("persist rows without schema with default type") {
    val sql = sqlContext
    import sql.implicits._

    sc.parallelize(data).toDF.write


    def data = (3 to 5).map { i =>
      HBaseRecordDynamic(i, "schema less")
    }

    writeData(data, HBaseRecordDynamic.catalog)

    // Test

    val result = withCatalog(HBaseRecordExtended.catalog, Map(
      HBaseRelation.RESTRICTIVE -> HBaseRelation.Restrictive.none,
      HBaseRelation.MAX_VERSIONS -> "3"
    ))


    val rows = result.take(10)


    assert(rows.size == 6)

    println(rows.mkString(" | "))

    assert(rows(0).size == 10)

  }

  test("read rows for the families declared in catalog") {
    val sql = sqlContext
    import sql.implicits._

    sc.parallelize(data).toDF.write


    def data = (3 to 5).map { i =>
      HBaseRecordDynamic(i, "schema less")
    }

    writeData(data, HBaseRecordDynamic.catalog)

    // Test

    val result = withCatalog(HBaseRecordExtended.catalogcf7, Map(
      HBaseRelation.RESTRICTIVE -> HBaseRelation.Restrictive.family,
      HBaseRelation.MAX_VERSIONS -> "3"
    ))


    val rows = result.take(10)


    assert(rows.size == 6)

    println(rows.mkString(" | "))

    assert(rows(0).size == 3)

  }

}
