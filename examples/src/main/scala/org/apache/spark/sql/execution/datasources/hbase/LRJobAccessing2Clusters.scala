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

case class LRecord(
    col0: String,
    col1: Boolean,
    col2: Double,
    col3: Float,
    col4: Int,
    col5: Long,
    col6: Short,
    col7: String,
    col8: Byte)

object LRecord {
  def apply(i: Int): LRecord = {
    val s = s"""row${"%03d".format(i)}"""
    LRecord(s,
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

// long running job to access 2 HBase clusters
object LRJobAccessing2Clusters {
  val cat1 = s"""{
                |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"PrimitiveType"},
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

  val cat2 = s"""{
                |"table":{"namespace":"default", "name":"shcExampleTable2", "tableCoder":"PrimitiveType"},
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


  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LRJobAccessing2Clusters <configurationFile1> <configurationFile2> [sleepTime]")
      System.exit(1)
    }

    // configuration file of HBase cluster
    val conf1 = args(0)
    val conf2 = args(1)
    val sleepTime = if (args.length > 2) args(2).toLong else 2 * 60 * 1000 // sleep 2 min by default

    val spark = SparkSession.builder()
      .appName("LRJobAccessing2Clusters")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    def withCatalog(cat: String, conf: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat, HBaseRelation.HBASE_CONFIGFILE -> conf))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    def saveData(cat: String, conf: String, data: Seq[LRecord]) = {
      sc.parallelize(data).toDF.write
        .options(Map(HBaseTableCatalog.tableCatalog -> cat,
          HBaseRelation.HBASE_CONFIGFILE -> conf, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }

    val timeEnd = System.currentTimeMillis() + (25 * 60 * 60 * 1000) // 25h later
    while (System.currentTimeMillis() < timeEnd) {
      // data saved into cluster 1
      val data1 = (0 to 120).map { i =>
        LRecord(i)
      }
      saveData(cat1, conf1, data1)

      // data saved into cluster 2
      val data2 = (100 to 200).map { i =>
        LRecord(i)
      }
      saveData(cat2, conf2, data2)

      val df1 = withCatalog(cat1, conf1)
      val df2 = withCatalog(cat2, conf2)
      val s1 = df1.filter($"col0" <= "row120" && $"col0" > "row090").select("col0", "col2")
      val s2 = df2.filter($"col0" <= "row150" && $"col0" > "row100").select("col0", "col5")
      val result = s1.join(s2, Seq("col0"))

      result.sort($"col0".asc, $"col2", $"col5").show()  // should be row101 to row120, as following:
      /*+------+-----+----+
      |  col0| col2|col5|
      +------+-----+----+
      |row101|101.0| 101|
      |row102|102.0| 102|
      |row103|103.0| 103|
      |row104|104.0| 104|
      |row105|105.0| 105|
      |row106|106.0| 106|
      |row107|107.0| 107|
      |row108|108.0| 108|
      |row109|109.0| 109|
      |row110|110.0| 110|
      |row111|111.0| 111|
      |row112|112.0| 112|
      |row113|113.0| 113|
      |row114|114.0| 114|
      |row115|115.0| 115|
      |row116|116.0| 116|
      |row117|117.0| 117|
      |row118|118.0| 118|
      |row119|119.0| 119|
      |row120|120.0| 120|
      +------+-----+----+ */

      Thread.sleep(sleepTime)
    }
    sc.stop()
  }
}
