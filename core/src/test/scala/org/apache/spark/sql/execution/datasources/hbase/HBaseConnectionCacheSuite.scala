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

package org.apache.spark.sql.execution.datasources.hbase

import java.util.concurrent.ExecutorService

import org.scalatest.FunSuite
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HConstants, TableName}

class HBaseConnectionKeyMocker(val confId: Int) extends HBaseConnectionKey(null) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[HBaseConnectionKeyMocker]

  override def equals(other: Any): Boolean = other match {
    case that: HBaseConnectionKeyMocker =>
      (that canEqual this) &&
        super.equals(that) &&
        confId == that.confId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), confId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

class ConnectionMocker extends Connection {
  var isClosed: Boolean = false

  def getRegionLocator(tableName: TableName): RegionLocator = null

  def getConfiguration: Configuration = null

  def getTable(tableName: TableName): Table = null

  def getTable(tableName: TableName, pool: ExecutorService): Table = null

  def getBufferedMutator(params: BufferedMutatorParams): BufferedMutator = null

  def getBufferedMutator(tableName: TableName): BufferedMutator = null

  def getAdmin: Admin = null

  def getTableBuilder(tableName: TableName, executorService: ExecutorService): TableBuilder = null

  def close(): Unit = {
    if (isClosed)
      throw new IllegalStateException()
    isClosed = true
  }

  def isAborted: Boolean = true

  def abort(why: String, e: Throwable) = {}
}

class HBaseConnectionCacheSuite extends FunSuite with Logging {
  /*
   * These tests must be performed sequentially as they operate with an
   * unique running thread and resource.
   *
   * It looks there's no way to tell FunSuite to do so, so making those
   * test cases normal functions which are called sequentially in a single
   * test case.
   */
  test("all test cases") {
    testBasic()
    testCache()
    testWithPressureWithoutClose()
    testWithPressureWithClose()
  }

  def cleanEnv(): Unit = {
    // reset stats when testing.
    HBaseConnectionCache.resetCache(resetStats = true)
  }

  def testBasic() {
    cleanEnv()
    HBaseConnectionCache.setTimeout(1 * 1000)

    val connKeyMocker1 = new HBaseConnectionKeyMocker(1)
    val connKeyMocker1a = new HBaseConnectionKeyMocker(1)
    val connKeyMocker2 = new HBaseConnectionKeyMocker(2)

    val c1 = HBaseConnectionCache
      .getConnection(connKeyMocker1, new ConnectionMocker)

    assert(HBaseConnectionCache.connectionMap.size === 1)
    assert(HBaseConnectionCache.getStat.numTotalRequests === 1)
    assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 1)
    assert(HBaseConnectionCache.getStat.numActiveConnections === 1)

    val c1a = HBaseConnectionCache
      .getConnection(connKeyMocker1a, new ConnectionMocker)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 1)
    }
    assert(HBaseConnectionCache.getStat.numTotalRequests === 2)
    assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 1)
    assert(HBaseConnectionCache.getStat.numActiveConnections === 1)

    val c2 = HBaseConnectionCache
      .getConnection(connKeyMocker2, new ConnectionMocker)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 2)
    }
    assert(HBaseConnectionCache.getStat.numTotalRequests === 3)
    assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 2)
    assert(HBaseConnectionCache.getStat.numActiveConnections === 2)

    c1.close()
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 2)
    }
    assert(HBaseConnectionCache.getStat.numActiveConnections === 2)

    c1a.close()
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 2)
    }
    assert(HBaseConnectionCache.getStat.numActiveConnections === 2)

    HBaseConnectionCache.sleep(3 * 1000) // Leave housekeeping thread enough time
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 1)
      assert(HBaseConnectionCache.connectionMap.iterator.next()._1
        .asInstanceOf[HBaseConnectionKeyMocker].confId === 2)
    }
    assert(HBaseConnectionCache.getStat.numActiveConnections === 1)

    c2.close()
  }

  def testCache(): Unit = {
    cleanEnv()

    val timeout = 100
    HBaseConnectionCache.setTimeout(timeout)
    HBaseConnectionCache.performHousekeeping(true)

    def createConfig(retryNumber: Int, clientPause: Long): Configuration = {
      val conf = new Configuration()
      conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, Integer.toString(retryNumber))
      conf.set(HConstants.HBASE_CLIENT_PAUSE, java.lang.Long.toString(clientPause))
      conf
    }

    val defaultRetryNumber = HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER
    val defaultClientPause = HConstants.DEFAULT_HBASE_CLIENT_PAUSE

    val connKey1 = new HBaseConnectionKey(createConfig(defaultRetryNumber, defaultClientPause))
    val connKey2 = new HBaseConnectionKey(createConfig(defaultRetryNumber, defaultClientPause + 1))
    val connKey3 = new HBaseConnectionKey(createConfig(defaultRetryNumber + 1, defaultClientPause + 1))
    val connKey3a = new HBaseConnectionKey(createConfig(defaultRetryNumber + 1, defaultClientPause + 1))

    val c1 = HBaseConnectionCache
      .getConnection(connKey1, new ConnectionMocker)
    val c2 = HBaseConnectionCache
      .getConnection(connKey2, new ConnectionMocker)
    val c3 = HBaseConnectionCache
      .getConnection(connKey3, new ConnectionMocker)
    val c3a = HBaseConnectionCache
      .getConnection(connKey3a, new ConnectionMocker)

    assert(c3 === c3a)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 3)
    }

    c1.close()
    c2.close()
    HBaseConnectionCache.sleep(timeout + 1)
    HBaseConnectionCache.performHousekeeping(false)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 1)
    }

    // force will remove even if in use.
    // c3.close()
    HBaseConnectionCache.performHousekeeping(true)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 0)
    }
    assert(c3.isClosed)
  }

  def testWithPressureWithoutClose() {
    cleanEnv()

    class TestThread extends Runnable {
      override def run() {
        for (i <- 0 to 999) {
          val c = HBaseConnectionCache.getConnection(
            new HBaseConnectionKeyMocker(Random.nextInt(10)), new ConnectionMocker)
        }
      }
    }

    HBaseConnectionCache.setTimeout(500)
    val threads: Array[Thread] = new Array[Thread](100)
    for (i <- 0 to 99) {
      threads.update(i, new Thread(new TestThread()))
      threads(i).run()
    }
    try {
      threads.foreach { x => x.join() }
    } catch {
      case e: InterruptedException => println(e.getMessage)
    }

    HBaseConnectionCache.sleep(1000)
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 10)
      assert(HBaseConnectionCache.getStat.numTotalRequests === 100 * 1000)
      assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 10)
      assert(HBaseConnectionCache.getStat.numActiveConnections === 10)
      var totalRc: Int = 0
      HBaseConnectionCache.connectionMap.foreach {
        x => totalRc += x._2.refCount
      }
      assert(totalRc === 100 * 1000)
      HBaseConnectionCache.connectionMap.foreach {
        x => {
          x._2.refCount = 0
          x._2.timestamp = System.currentTimeMillis() - 1000
        }
      }
    }
    HBaseConnectionCache.sleep(1000)
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 0)
    }
    assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 10)
    assert(HBaseConnectionCache.getStat.numActiveConnections === 0)
  }

  def testWithPressureWithClose() {
    cleanEnv()

    class TestThread extends Runnable {
      override def run() {
        for (i <- 0 to 999) {
          val c = HBaseConnectionCache.getConnection(
            new HBaseConnectionKeyMocker(Random.nextInt(10)), new ConnectionMocker)
          Thread.`yield`()
          c.close()
        }
      }
    }

    HBaseConnectionCache.setTimeout(3 * 1000)
    val threads: Array[Thread] = new Array[Thread](100)
    for (i <- threads.indices) {
      threads.update(i, new Thread(new TestThread()))
      threads(i).run()
    }
    try {
      threads.foreach { x => x.join() }
    } catch {
      case e: InterruptedException => println(e.getMessage)
    }

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 10)
    }
    assert(HBaseConnectionCache.getStat.numTotalRequests === 100 * 1000)
    assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 10)
    assert(HBaseConnectionCache.getStat.numActiveConnections === 10)

    HBaseConnectionCache.sleep(6 * 1000)
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 0)
    }
    assert(HBaseConnectionCache.getStat.numActualConnectionsCreated === 10)
    assert(HBaseConnectionCache.getStat.numActiveConnections === 0)
  }
}
