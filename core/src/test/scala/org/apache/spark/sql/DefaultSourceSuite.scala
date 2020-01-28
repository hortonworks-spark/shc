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

import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.PrunedFilteredScan

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

  def unpadded(i: Int, t: String): HBaseRecord = {
    val s = s"""row${i}"""
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

  def withCatalog(cat: String, options: Map[String, String] = Map.empty): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat) ++ options)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  private def prunedFilterScan(cat: String): PrunedFilteredScan = {
    HBaseRelation(Map(HBaseTableCatalog.tableCatalog->cat),None)(sqlContext)
  }

  def persistDataInHBase(cat: String, data: Seq[HBaseRecord], options: Map[String, String] = Map.empty): Unit = {
    val sql = sqlContext
    import sql.implicits._
    sc.parallelize(data).toDF.write
      .options(Map(
        HBaseTableCatalog.newTable -> "5",
        HBaseTableCatalog.tableCatalog -> cat
      ) ++ options)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def testDistribution(tableName: String, minimumValue: String, maximumValue: String): Boolean = {
    val admin = htu.getConnection().getAdmin()
    val regions = admin.getRegions(TableName.valueOf(tableName))
    val minimumBytes = Bytes.toBytes(minimumValue)
    val maximumBytes = Bytes.toBytes(maximumValue)

    val minimumRegion = regions.filter(r => r.containsRow(minimumBytes)).head
    val maximumRegion = regions.filter(r => r.containsRow(maximumBytes)).head

    minimumRegion.getRegionName() != maximumRegion.getRegionName()
  }

  test("populate table") {
    //createTable(tableName, columnFamilies)
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      HBaseRecord(i, "extra")
    }
    persistDataInHBase(catalog, data)
  }

  test("population distrubtion for alphabet") {
    val sql = sqlContext
    import sql.implicits._

    val rawData = "acegikmoq".permutations.toList
    val data = rawData.map { i =>
      HBaseRecord(5, i)
    }
    persistDataInHBase(catalog, data)
    assert(testDistribution(tableName, rawData.min(Ordering.String), rawData.max(Ordering.String)))
  }

  test("population distrubtion for numbers") {
    val sql = sqlContext
    import sql.implicits._

    val numericTable = "numericTestTable"
    val numericCatalog = defineCatalog(numericTable)
    val rawData = (1 to 100000).map(_.toString).toList
    val minValue = rawData.min(Ordering.String)
    val maxValue = rawData.max(Ordering.String)
    val data = rawData.map { i =>
      HBaseRecord(5, i)
    }
    val options = Map(HBaseTableCatalog.minTableSplitPoint -> minValue, HBaseTableCatalog.maxTableSplitPoint -> maxValue)
    persistDataInHBase(numericCatalog, data, options)
    assert(testDistribution(numericTable, minValue, maxValue))
  }

  test("empty column") {
    val df = withCatalog(catalog)
    df.createOrReplaceTempView("table0")
    val c = sqlContext.sql("select count(1) from table0").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("IN and Not IN filter1") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col0" isin ("row005", "row001", "row002")) and !($"col0" isin ("row001", "row002")))
      .select("col0")
    s.explain(true)
    s.show
    assert(s.count() == 1)
  }

  test("IN and Not IN filter2") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col0" isin ("row055", "row001", "row002")) and !($"col0" < "row005"))
      .select("col0")
    s.explain(true)
    s.show
    assert(s.count() == 1)
  }

  test("IN filter stack overflow") {
    val df = withCatalog(catalog)
    val items = (0 to 2000).map{i => s"xaz$i"}
    val filterInItems = Seq("row001") ++: items

    val s = df.filter($"col0" isin(filterInItems:_*)).select("col0")
    s.explain(true)
    s.show()
    assert(s.count() == 1)
  }

  test("NOT IN filter stack overflow") {
    val df = withCatalog(catalog)
    val items = (0 to 2000).map{i => s"xaz$i"}
    val filterNotInItems = items

    val s = df.filter(not($"col0" isin(filterNotInItems:_*))).select("col0")
    s.explain(true)
    s.show
    assert(s.count() == df.count())
  }

  test("IN filter, RDD") {
    val scan = prunedFilterScan(catalog)
    val columns = Array("col0")
    val filters =
      Array[org.apache.spark.sql.sources.Filter](
        org.apache.spark.sql.sources.In("col0", Array("row001")))
    val rows = scan.buildScan(columns,filters).collect()
    assert(rows.length == 1)
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
    df.createOrReplaceTempView("table1")
    val c = sqlContext.sql("select col1, col0 from table1 where col4 = 5")
    c.show()
    assert(c.count == 1)
  }

  test("agg query") {
    val df = withCatalog(catalog)
    df.createOrReplaceTempView("table1")
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
    val sql = sqlContext
    import sql.implicits._

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


    persistDataInHBase(catalog, oldData, Map(HBaseRelation.TIMESTAMP -> oldMs.toString))
    persistDataInHBase(catalog, newData)

    // Test specific timestamp -- Full scan, Timestamp
    val individualTimestamp = withCatalog(catalog, Map(HBaseRelation.TIMESTAMP -> oldMs.toString))
    assert(individualTimestamp.count() == 101)

    // Test getting everything -- Full Scan, No range
    val everything = withCatalog(catalog)
    assert(everything.count() == 256)
    // Test getting everything -- Pruned Scan, TimeRange
    val element50 = everything.where(col("col0") === lit("row050")).select("col7").collect()(0)(0)
    assert(element50 == "String50: extra")
    val element200 = everything.where(col("col0") === lit("row200")).select("col7").collect()(0)(0)
    assert(element200 == "String200: new")

    // Test Getting old stuff -- Full Scan, TimeRange
    val oldRange = withCatalog(catalog, Map(HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (oldMs + 100).toString))
    assert(oldRange.count() == 101)
    // Test Getting old stuff -- Pruned Scan, TimeRange
    val oldElement50 = oldRange.where(col("col0") === lit("row050")).select("col7").collect()(0)(0)
    assert(oldElement50 == "String50: old")

    // Test Getting middle stuff -- Full Scan, TimeRange
    val middleRange = withCatalog(catalog, Map(HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (startMs + 100).toString))
    assert(middleRange.count() == 256)
    // Test Getting middle stuff -- Pruned Scan, TimeRange
    val middleElement200 = middleRange.where(col("col0") === lit("row200")).select("col7").collect()(0)(0)
    assert(middleElement200 == "String200: extra")
  }

  test("Variable sized keys") {
    val sql = sqlContext
    import sql.implicits._
    val data = (0 to 100).map { i =>
      HBaseRecord.unpadded(i, "old")
    }

    // Delete the table because for this test we want to check the different number of values
    htu.deleteTable(TableName.valueOf(tableName))
    createTable(tableName, columnFamilies)

    persistDataInHBase(catalog, data)

    val keys = withCatalog(catalog).select("col0").distinct().collect().map(a => a.getString(0))
    // There was an issue with the keys being truncated during buildrow.
    // This would result in only the number of keys as large as the first one
    assert(keys.length == 101)
    assert(keys.contains("row0"))
    assert(keys.contains("row100"))
    assert(keys.contains("row57"))
  }

  test("No need to specify 'HBaseTableCatalog.newTable' option when saving data into an existing table") {
    val sql = sqlContext
    import sql.implicits._

    val df1 = withCatalog(catalog)
    assert(df1.count() == 101)

    // add three more records to the existing table "table1" which has 101 records
    val data = (256 to 258).map { i =>
      HBaseRecord(i, "extra")
    }
    persistDataInHBase(catalog, data)

    val df2 = withCatalog(catalog)
    assert(df2.count() == 104)
  }

  test("inserting data with null values") {
    val withNullData = (1 to 2).map(HBaseRecord(_, "").copy(col7 = null))
    val withoutNullData = (3 to 4).map(HBaseRecord(_, "not null"))

    val testCatalog = defineCatalog("testInsertNull")
    persistDataInHBase(testCatalog, withNullData ++ withoutNullData)

    val data: DataFrame = withCatalog(testCatalog)

    assert(data.count() == 4)

    val rows = data.take(10)
    assert(rows.count(_.getString(7) == null) == 2)
    assert(rows.count(_.getString(7) != null) == 2)
  }
}
