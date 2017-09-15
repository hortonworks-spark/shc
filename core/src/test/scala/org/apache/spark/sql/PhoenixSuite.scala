/*
 * (C) 2017 Hortonworks, Inc. All rights reserved. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. This file is licensed to You under the Apache License, Version 2.0
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

import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

case class PRecord(
    col0: String,
    col1: Boolean,
    col2: Double,
    col3: Float,
    col4: Int,
    col5: Long,
    col6: Short,
    col7: String,
    col8: Byte)

object PRecord {
  def apply(i: Int, t: String): PRecord = {
    val s = s"""row${"%03d".format(i)}"""
    PRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i: $t",
      i.toByte)
  }

  def unpadded(i: Int, t: String): PRecord = {
    val s = s"""row${i}"""
    PRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i: $t",
      i.toByte)
  }
}

class PhoenixSuite extends SHC with Logging {
  override def catalog = s"""{
                    |"table":{"namespace":"default", "name":"phoenixTable",
                    |  "tableCoder":"Phoenix", "version":"2.0"},
                    |"rowkey":"key",
                    |"columns":{
                    |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                    |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                    |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                    |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                    |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                    |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                    |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                    |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                    |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                    |}
                    |}""".stripMargin


  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("populate table") {
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      PRecord(i, "extra")
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("full query") {
    val df = withCatalog(catalog)
    df.show
    assert(df.count() == 256)
    assert(df.first().getByte(8) == 0)
  }

  test("empty column") {
    val df = withCatalog(catalog)
    df.registerTempTable("table0")
    val c = sqlContext.sql("select count(1) from table0").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("IN and Not IN filter1") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col0" isin ("row005", "row001", "row002")) and !($"col0" isin ("row001", "row002")))
      .select("col0", "col8")
    s.explain(true)
    s.show
    assert(s.count() == 1)
    assert(s.first().getByte(1) == 5)
  }
}
