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

import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.functions._

case class HBaseRecord(
    col0: String,
    col1: Boolean,
    col2: Double,
    col3: Float,
    col4: Int,
    col5: Long,
    col6: Short,
    col7: String,
    col8: Byte)

object HBaseRecord {
  def apply(i: Int, t: String): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
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

class DefaultSourceSuite extends SHC with Logging {
  val sc = new SparkContext("local", "HBaseTest", conf)
  val sqlContext = new SQLContext(sc)

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("populate table") {
    //createTable(tableName, columnFamilies)
    import sqlContext.implicits._

    val data = (0 to 255).map { i =>
      HBaseRecord(i, "extra")
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("empty column") {
    val df = withCatalog(catalog)
    df.registerTempTable("table0")
    val c = sqlContext.sql("select count(1) from table0").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("full query") {
    val df = withCatalog(catalog)
    df.show
    assert(df.count() == 256)
  }

  test("filtered query0") {
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= "row005")
      .select("col0", "col1")
    s.show
    assert(s.count() == 6)
  }


  test("filtered query1") {
    val df = withCatalog(catalog)
    val s = df.filter($"col0" === "row005" || $"col0" <= "row005")
      .select("col0", "col1")
    s.show
    assert(s.count() == 6)
  }


  test("filtered query2") {
    val df = withCatalog(catalog)
    val s = df.filter($"col0" === "row005" || $"col0" >= "row005")
      .select("col0", "col1")
    s.show
    assert(s.count() == 251)
  }

  test("filtered query3") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005")
      .select("col0", "col1")
    s.show
    assert(s.count() == 17)
  }

  test("filtered query4") {
    val df = withCatalog(catalog)
    df.registerTempTable("table1")
    val c = sqlContext.sql("select col1, col0 from table1 where col4 = 5")
    c.show()
    assert(c.count == 1)
  }

  test("agg query") {
    val df = withCatalog(catalog)
    df.registerTempTable("table1")
    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    c.show()
    assert(c.collect.apply(0).apply(0).asInstanceOf[Long] == 50)
  }

  test("complicate filtered query") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      ($"col4" === 1 ||
      $"col4" === 42))
      .select("col0", "col1", "col4")
    s.show
    assert(s.count() == 2)
  }

  test("complicate filtered query1") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") ||
      ($"col4" === 1 ||
        $"col4" === 42))
      .select("col0", "col1", "col4")
    s.show
    assert(s.count() == 17)
  }

  test("String contains filter") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      $"col7".contains("String3"))
    .select("col0", "col1", "col7")
    s.show
    assert(s.count() == 1)
  }

  test("String not contains filter") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      !$"col7".contains("String3"))
      .select("col0", "col1", "col7")
    s.show
    assert(s.count() == 16)
  }

  test("Or filter") {
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= "row050" || $"col7".contains("String60"))
      .select("col0", "col1", "col7")
    s.show
    assert(s.count() == 52)
  }

  test("Timestamp semantics") {
    import sqlContext.implicits._

    // There's already some data in here from recently. Let's throw something in
    // from 1993 which we can include/exclude and add some data with the implicit (now) timestamp.
    // Then we should be able to cross-section it and only get points in between, get the most recent view
    // and get an old view.
    val oldMs = 754869600000L
    val startMs = System.currentTimeMillis()
    val oldData = (0 to 100).map { i =>
      HBaseRecord(i, "old")
    }
    val newData = (200 to 255).map { i =>
      HBaseRecord(i, "new")
    }

    sc.parallelize(oldData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.tableName -> "5", HBaseRelation.TIMESTAMP -> oldMs.toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sc.parallelize(newData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.tableName -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    // Test specific timestamp -- Full scan, Timestamp
    val individualTimestamp = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog, HBaseRelation.TIMESTAMP -> oldMs.toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load();
    assert(individualTimestamp.count() == 101)

    // Test getting everything -- Full Scan, No range
    val everything = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    assert(everything.count() == 256)
    // Test getting everything -- Pruned Scan, TimeRange
    val element50 = everything.where(col("col0") === lit("row050")).select("col7").collect()(0)(0)
    assert(element50 == "String50: extra")
    val element200 = everything.where(col("col0") === lit("row200")).select("col7").collect()(0)(0)
    assert(element200 == "String200: new")

    // Test Getting old stuff -- Full Scan, TimeRange
    val oldRange = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog, HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (oldMs + 100).toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    assert(oldRange.count() == 101)
    // Test Getting old stuff -- Pruned Scan, TimeRange
    val oldElement50 = oldRange.where(col("col0") === lit("row050")).select("col7").collect()(0)(0)
    assert(oldElement50 == "String50: old")

    // Test Getting middle stuff -- Full Scan, TimeRange
    val middleRange = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog, HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (startMs + 100).toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    assert(middleRange.count() == 256)
    // Test Getting middle stuff -- Pruned Scan, TimeRange
    val middleElement200 = middleRange.where(col("col0") === lit("row200")).select("col7").collect()(0)(0)
    assert(middleElement200 == "String200: extra")
  }
}
