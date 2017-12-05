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

import org.apache.spark.sql.execution.datasources.hbase.Logging

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.spark.sql.execution.datasources.hbase.SparkHBaseConf
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class SHC  extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging {
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  var spark: SparkSession = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  var df: DataFrame = null

  private[spark] var htu = new HBaseTestingUtility
  private[spark] def tableName = "table1"

  private[spark] def columnFamilies: Array[String] = Array.tabulate(9){ x=> s"cf$x"}
  var table: Table = null
  val conf = new SparkConf
  conf.set(SparkHBaseConf.testConf, "true")
  // private[spark] var columnFamilyStr = Bytes.toString(columnFamily)

  def defineCatalog(tName: String) = s"""{
                                         |"table":{"namespace":"default", "name":"$tName"},
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

  @deprecated(since = "04.12.2017(dd/mm/year)", message = "use `defineCatalog` instead")
  def catalog = defineCatalog(tableName)

  override def beforeAll() {
    val tempDir: File = Files.createTempDir
    tempDir.deleteOnExit
    htu.startMiniCluster
    SparkHBaseConf.conf = htu.getConfiguration
    logInfo(" - minicluster started")
    println(" - minicluster started")

    spark = SparkSession.builder()
      .master("local")
      .appName("HBaseTest")
      .config(conf)
      .getOrCreate()

    sqlContext = spark.sqlContext
    sc = spark.sparkContext
  }

  override def afterAll() {
    htu.shutdownMiniCluster()
    spark.stop()
  }

  def createTable(name: String, cfs: Array[String]) {
    val tName = Bytes.toBytes(name)
    val bcfs = cfs.map(Bytes.toBytes(_))
    try {
      htu.deleteTable(TableName.valueOf(tName))
    } catch {
      case _ : Throwable =>
        logInfo(" - no table " + name + " found")
    }
    htu.createMultiRegionTable(TableName.valueOf(tName), bcfs)
  }


  def createTable(name: Array[Byte], cfs: Array[Array[Byte]]) {
    try {
      htu.deleteTable(TableName.valueOf(name))
    } catch {
      case _ : Throwable =>
        logInfo(" - no table " + Bytes.toString(name) + " found")
    }
    htu.createMultiRegionTable(TableName.valueOf(name), cfs)
  }
}
