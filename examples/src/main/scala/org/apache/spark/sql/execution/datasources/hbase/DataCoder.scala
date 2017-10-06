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

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DCRecord(
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

object DCRecord {
  def apply(i: Int): DCRecord = {
    DCRecord(s"row${"%03d".format(i)}",
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

object DataCoder {
  def cat = s"""{
                |"table":{"namespace":"default", "name":"shcExampleDCTable", "tableCoder":"Phoenix", "version":"2.0"},
                |"rowkey":"key1:key2",
                |"columns":{
                |"col00":{"cf":"rowkey", "col":"key1", "type":"string"},
                |"col01":{"cf":"rowkey", "col":"key2", "type":"int"},
                |"col1":{"cf":"CF1", "col":"COL1", "type":"boolean"},
                |"col2":{"cf":"CF1", "col":"COL2", "type":"double"},
                |"col3":{"cf":"CF2", "col":"COL3", "type":"float"},
                |"col4":{"cf":"CF2", "col":"COL4", "type":"int"},
                |"col5":{"cf":"CF3", "col":"COL5", "type":"bigint"},
                |"col6":{"cf":"CF3", "col":"COL6", "type":"smallint"},
                |"col7":{"cf":"CF4", "col":"COL7", "type":"string"},
                |"col8":{"cf":"CF4", "col":"COL8", "type":"tinyint"}
                |}
                |}""".stripMargin

  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("DataCoderExample")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    // populate table with composite key
    val data = (0 to 255).map { i =>
      DCRecord(i)
    }

    sc.parallelize(data).toDF.write
      .options(Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    val df = withCatalog(cat)
    df.show
    df.filter($"col0" <= "row005")
      .select($"col0", $"col1").show
    df.filter($"col0" === "row005" || $"col0" <= "row005")
      .select($"col0", $"col1").show
    df.filter($"col0" > "row250")
      .select($"col0", $"col1").show
    df.registerTempTable("table1")
    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    c.show()
  }
}
