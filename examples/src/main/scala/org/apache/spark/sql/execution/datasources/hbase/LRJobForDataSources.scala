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

package org.apache.spark.sql.execution.datasources.hbase.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}

case class LRRecord(
    key: Int,
    col1: Boolean,
    col2: Double,
    col3: Float)

object LRRecord {
  def apply(i: Int): LRRecord = {
    LRRecord(i,
      i % 2 == 0,
      i.toDouble,
      i.toFloat)
  }
}

// long running job for different data sources
object LRJobForDataSources {
  val cat = s"""{
            |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
            |"rowkey":"key",
            |"columns":{
              |"key":{"cf":"rowkey", "col":"key", "type":"int"},
              |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
              |"col3":{"cf":"cf3", "col":"col3", "type":"float"}
            |}
          |}""".stripMargin

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: LRJobAccessing2Clusters <hiveTableName> [sleepTime]")
      System.exit(1)
    }

    val hiveTableName = args(0)
    val sleepTime = if (args.length > 1) args(1).toLong else 2 * 60 * 1000 // sleep 2 min by default

    val spark = SparkSession.builder()
      .appName("LRJobForDataSources")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._
    import spark.sql

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val timeEnd = System.currentTimeMillis() + (25 * 60 * 60 * 1000) // 25h later
    while (System.currentTimeMillis() < timeEnd) {
      // Part 1: write data into Hive table and read data from it, which accesses HDFS
      sql(s"DROP TABLE IF EXISTS $hiveTableName")
      sql(s"CREATE TABLE $hiveTableName(key INT, col1 BOOLEAN, col2 DOUBLE, col3 FLOAT)")
      for (i <- 1 to 3) {
        sql(s"INSERT INTO $hiveTableName VALUES ($i, ${i % 2 == 0}, ${i.toDouble}, ${i.toFloat})")
      }
      val df1 = sql(s"SELECT * FROM $hiveTableName")
      df1.show()

      // Part 2: create HBase table, write data into it, read data from it
      val data = (0 to 40).map { i =>
        LRRecord(i)
      }
      sc.parallelize(data).toDF.write.options(
        Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
      val df2 = withCatalog(cat)
      df2.show
      df2.filter($"key" <= "5").select($"key", $"col1").show

      // Part 3: join the two dataframes
      val s1 = df1.filter($"key" <= "40").select("key", "col1")
      val s2 = df2.filter($"key" <= "20" && $"key" >= "1").select("key", "col2")
      val result =  s1.join(s2, Seq("key"))
      result.show()

      Thread.sleep(sleepTime)
    }

    spark.stop()
  }
}
