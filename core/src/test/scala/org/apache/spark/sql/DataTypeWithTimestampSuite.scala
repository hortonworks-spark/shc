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

import java.time.Instant

import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, Logging}

class DataTypeWithTimestampSuite extends SHC with Logging {

  var saveTS: Long = _

  override def catalog = s"""{
            |"table":{"namespace":"default", "name":"table1", "tableCoder":"PrimitiveType"},
            |"rowkey":"key",
            |"timestamp":"ts",
            |"columns":{
              |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
              |"colT":{"cf":"timestamp", "col":"ts", "type":"long"},
              |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              |"col2":{"cf":"cf1", "col":"col2", "type":"double"},
              |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
              |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
              |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
              |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
              |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
              |"col8":{"cf":"cf7", "col":"col8", "type":"tinyint"}
            |}
          |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  override def beforeAll() {
    super.beforeAll()
    println("populate table")
    //createTable(tableName, columnFamilies)
    val sql = sqlContext
    import sql.implicits._

    val data = (0 until 32).map { i =>
      IntKeyRecord(i)
    }
    saveTS = Instant.now.toEpochMilli
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("less than 0") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" < 0)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 16)
  }

  test("lessequal than -10") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= -10)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 11)
  }

  test("lessequal than -9") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= -9)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 12)
  }

  test("greaterequal than -9") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" >= -9)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 21)
  }

  test("greaterequal than 0") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" >= 0)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 16)
  }

  test("greater than 10") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" > 10)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 10)
  }

  test("and") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" > -10 && $"col0" <= 10)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 11)
  }

  test("or") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= -10 || $"col0" > 10)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 21)
  }

  test("all") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    val s = df.filter($"col0" >= -100)
    s.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(s.count() == 32)
  }

  test("full query") {
    val testTS = Instant.now.toEpochMilli
    val df = withCatalog(catalog)
    df.show
    assert(df.columns.contains("colT"))
    assert(df.filter($"colT" < saveTS && $"colT" > testTS).count() == 0)
    assert(df.count() == 32)
  }
}
