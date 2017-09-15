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

case class PCompositeRecord(
    col00: String,
    col01: Int,
    col1: Boolean,
    col2: Double,
    col3: Float,
    col4: Int,
    col5: Long,
    col6: Short,
    col7: String,
    col8: Byte)

object PCompositeRecord {
  def apply(i: Int): PCompositeRecord = {
    PCompositeRecord(s"row${"%03d".format(i)}",
      if (i % 2 == 0) {
        i
      } else {
        -i
      },
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

class PhoenixCompositeKeySuite extends SHC with Logging {
  override def catalog = s"""{
                             |"table":{"namespace":"default", "name":"table1", "tableCoder":"Phoenix"},
                             |"rowkey":"key1:key2",
                             |"columns":{
                             |"col00":{"cf":"rowkey", "col":"key1", "type":"string"},
                             |"col01":{"cf":"rowkey", "col":"key2", "type":"int"},
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

  test("populate table with composite key") {
    //createTable(tableName, columnFamilies)
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      HBaseCompositeRecord(i)
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
  }

  test("filtered query1") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" <= "row050" && $"col01" > 40)
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 5)
  }

  test("filtered query2") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" <= "row050" && $"col01" >= 40)
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 6)
  }

  test("filtered query3") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" >= "row250" && $"col01" < 50)
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 3)
  }

  test("filtered query4") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" <= "row010")    // row005 not included
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 11)
  }

  test("filtered query5") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" === "row010")    // row005 not included
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 1)
  }
  test("filtered query51") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" === "row011")    // row005 not included
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 1)
  }

  test("filtered query52") {
    val df = withCatalog(catalog)
    val s = df.filter($"col00" === "row005")    // row005 not included
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 1)
  }


  test("filtered query6") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col00" <= "row050" && $"col00" > "row040") ||
      $"col00" === "row010" || // no included, since it is composite key
      $"col00" === "row020" || // not inlcuded
      $"col00" ===  "r20" ||   // not included
      $"col00" <= "row010")    // row005 not included
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 22)
  }


  test("filtered query7") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col00" <= "row050" && $"col00" > "row040") ||
      $"col00" === "row005" || // no included, since it is composite key
      $"col00" === "row020" || // not inlcuded
      $"col00" ===  "r20" ||   // not included
      $"col00" <= "row005")    // row005 not included
      .select("col00", "col01","col1")
    s.show
    assert(s.count() == 17)
  }
}
