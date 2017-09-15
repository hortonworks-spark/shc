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

import java.io.File

import scala.collection.JavaConverters._

import com.google.common.io.Files
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class HBaseTestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging {
  private[spark] var htu = HBaseTestingUtility.createLocalHTU()
  private[spark] var tableName: Array[Byte] = Bytes.toBytes("t1")
  private[spark] var columnFamily: Array[Byte] = Bytes.toBytes("cf0")
  private[spark] var columnFamilies: Array[Array[Byte]] =
    Array(Bytes.toBytes("cf0"), Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3"), Bytes.toBytes("cf4"))
  var table: Table = null
  // private[spark] var columnFamilyStr = Bytes.toString(columnFamily)

  override def beforeAll() {
    val tempDir: File = Files.createTempDir
    tempDir.deleteOnExit
    htu.cleanupTestDir
    htu.startMiniZKCluster
    htu.startMiniHBaseCluster(1, 4)
    logInfo(" - minicluster started")
    println(" - minicluster started")
    try {
      htu.deleteTable(TableName.valueOf(tableName))

      //htu.createTable(TableName.valueOf(tableName), columnFamily, 2, Bytes.toBytes("abc"), Bytes.toBytes("xyz"), 2)
    } catch {
      case _ : Throwable =>
        logInfo(" - no table " + Bytes.toString(tableName) + " found")
    }
    setupTable()
  }



  override def afterAll() {
    try {
      table.close()
      println("shutdown")
      htu.deleteTable(TableName.valueOf(tableName))
      logInfo("shuting down minicluster")
      htu.shutdownMiniHBaseCluster
      htu.shutdownMiniZKCluster
      logInfo(" - minicluster shut down")
      htu.cleanupTestDir
    } catch {
      case _ : Throwable => logError("teardown error")
    }
  }

  def setupTable() {
    val config = htu.getConfiguration
    htu.createMultiRegionTable(TableName.valueOf(tableName), columnFamilies)
    println("create htable t1")
    val connection = ConnectionFactory.createConnection(config)
    val r = connection.getRegionLocator(TableName.valueOf("t1"))
    table = connection.getTable(TableName.valueOf("t1"))

    val regionLocations = r.getAllRegionLocations.asScala.toSeq
    println(s"$regionLocations size: ${regionLocations.size}")
    (0 until 100).foreach { x =>
      var put = new Put(Bytes.toBytes(s"row$x"))
      (0 until 5).foreach { y =>
        put.addColumn(columnFamilies(y), Bytes.toBytes(s"c$y"), Bytes.toBytes(s"value $x $y"))
      }
      table.put(put)
    }
  }
}
