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

import java.io.{Closeable, IOException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.ipc.RpcControllerFactory
import org.apache.hadoop.hbase.security.{User, UserProvider}

private[spark] object HBaseConnectionCache extends Logging {

  // A hashmap of Spark-HBase connections. Key is HBaseConnectionKey.
  private[hbase] val connectionMap = new mutable.HashMap[HBaseConnectionKey, SmartConnection]()

  private val cacheStat = HBaseConnectionCacheStat(0, 0, 0)

  // in milliseconds
  private final val DEFAULT_TIME_OUT: Long = SparkHBaseConf.connectionCloseDelay
  private val timeout = new AtomicLong(DEFAULT_TIME_OUT)
  private val closed = new AtomicBoolean(false)

  private var housekeepingThread = new Thread(new Runnable {
    override def run() {
      while (true) {
        sleep(timeout.get(), allowInterrupt = true, allowClosed = true)
        if (closed.get()) return
        performHousekeeping(false)
      }
    }
  })
  housekeepingThread.setDaemon(true)
  housekeepingThread.start()

  // Thread.sleep can be spuriously woken up, this ensure we sleep for atleast the
  // 'duration' specified
  private[hbase] def sleep(duration: Long, allowInterrupt: Boolean = false, allowClosed: Boolean = false): Unit = {
    val startTime = System.currentTimeMillis()
    var remaining = duration
    while (remaining > 0) {
      try {
        Thread.sleep(remaining)
      } catch {
        case ex: InterruptedException if allowInterrupt => return
        case ex: Exception => // ignore
      }
      if (allowClosed && closed.get()) return
      val now = System.currentTimeMillis()
      remaining = duration - (now - startTime)
    }
  }


  def getStat: HBaseConnectionCacheStat = {
    connectionMap.synchronized {
      cacheStat.setActiveConnections(connectionMap.size)
      cacheStat.copy()
    }
  }

  // resetStats == true for testing, otherwise, it is not modified.
  private[hbase] def resetCache(resetStats: Boolean = false): Unit = {
    connectionMap.synchronized {
      if (closed.get()) return
      connectionMap.values.foreach(conn => IOUtils.closeQuietly(conn) )
      connectionMap.clear()
      if (resetStats) cacheStat.reset()
    }
  }

  def close(): Unit = {
    connectionMap.synchronized {
      if (closed.get()) return
      try {
        housekeepingThread.interrupt()
        resetCache()
      } finally {
        housekeepingThread = null
        closed.set(true)
      }
    }
  }

  private[hbase] def performHousekeeping(forceClean: Boolean) = {
    val tsNow: Long = System.currentTimeMillis()
    val connTimeout = timeout.get()
    connectionMap.synchronized {
      connectionMap.retain {
        (key, conn) => {
          if(conn.refCount < 0) {
            logError("Bug to be fixed: negative refCount")
          }

          if(forceClean || ((conn.refCount <= 0) && (tsNow - conn.timestamp > connTimeout))) {
            IOUtils.closeQuietly(conn.connection)
            false
          } else {
            true
          }
        }
      }
    }
  }

  // For testing purpose only
  def getConnection(key: HBaseConnectionKey, conn: => Connection): SmartConnection = {
    connectionMap.synchronized {
      if (closed.get()) return null
      val sc = connectionMap.getOrElseUpdate(key, {
        cacheStat.incrementActualConnectionsCreated(1)
        new SmartConnection(conn)
      })
      cacheStat.incrementTotalRequests(1)
      sc.refCount += 1
      sc
    }
  }

  def getConnection(conf: Configuration): SmartConnection =
    getConnection(new HBaseConnectionKey(conf), ConnectionFactory.createConnection(conf))

  // For testing purpose only
  def setTimeout(to: Long) : Unit = {
    connectionMap.synchronized {
      if (closed.get()) return
      timeout.set(to)
      housekeepingThread.interrupt()
    }
  }
}

private[hbase] class SmartConnection (
    val connection: Connection, var refCount: Int = 0, var timestamp: Long = 0) extends Closeable {
  def getTable(tableName: TableName): Table = connection.getTable(tableName)
  def getRegionLocator(tableName: TableName): RegionLocator = connection.getRegionLocator(tableName)
  def isClosed: Boolean = connection.isClosed
  def getAdmin: Admin = connection.getAdmin
  def close() = {
    HBaseConnectionCache.connectionMap.synchronized {
      refCount -= 1
      if(refCount <= 0)
        timestamp = System.currentTimeMillis()
    }
  }
}

/**
  * Denotes a unique key to an HBase Connection instance.
  * Please refer to 'org.apache.hadoop.hbase.client.HConnectionKey'.
  *
  * In essence, this class captures the properties in Configuration
  * that may be used in the process of establishing a connection.
  *
  */
class HBaseConnectionKey(c: Configuration) extends Logging {
  import HBaseConnectionKey.CONNECTION_PROPERTIES

  val (username, properties) = {
    var user: String = null

    val confMap = {
      if (c != null) {
        try {
          val provider: UserProvider = UserProvider.instantiate(c)
          val currentUser: User = provider.getCurrent
          if (currentUser != null) {
            user = currentUser.getName
          }
        }
        catch {
          case e: IOException =>
            logWarning("Error obtaining current user, skipping username in HBaseConnectionKey", e)
        }

        CONNECTION_PROPERTIES.flatMap(key =>
          Option(c.get(key)).map(value => key -> value)
        ).toMap
      } else {
        Map[String, String]()
      }
    }
    (user, confMap)
  }

  override def toString: String = {
    s"HBaseConnectionKey{username='$username, properties=$properties}"
  }

  override def equals(other: Any): Boolean = other match {
    case that: HBaseConnectionKey =>
        username == that.username &&
          CONNECTION_PROPERTIES.forall(key => this.properties.get(key) == that.properties.get(key))
    case _ => false
  }

  override def hashCode(): Int = {
    val userHashCode = if (null != username) username.hashCode else 0
    CONNECTION_PROPERTIES.flatMap(k => properties.get(k)).
      foldLeft(userHashCode)((a, b) => 31 * a + (if (null != b) b.hashCode else 0))
  }
}

private[hbase] object HBaseConnectionKey {
  private val CONNECTION_PROPERTIES: Array[String] = Array[String](
    HConstants.ZOOKEEPER_QUORUM,
    HConstants.ZOOKEEPER_ZNODE_PARENT,
    HConstants.ZOOKEEPER_CLIENT_PORT,
    HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
    HConstants.HBASE_CLIENT_PAUSE,
    HConstants.HBASE_CLIENT_RETRIES_NUMBER,
    HConstants.HBASE_RPC_TIMEOUT_KEY,
    HConstants.HBASE_META_SCANNER_CACHING,
    HConstants.HBASE_CLIENT_INSTANCE_ID,
    HConstants.RPC_CODEC_CONF_KEY,
    HConstants.USE_META_REPLICAS,
    RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY)

}


/**
 * To log the state of [[HBaseConnectionCache]]
 *
 * numTotalRequests: number of total connection requests to the cache
 * numActualConnectionsCreated: number of actual HBase connections the cache ever created
 * numActiveConnections: number of current alive HBase connections the cache is holding
 */
class HBaseConnectionCacheStat(private var _numTotalRequests: Long,
    private var _numActualConnectionsCreated: Long,
    private var _numActiveConnections: Long) {

  def numTotalRequests: Long = _numTotalRequests
  def numActualConnectionsCreated: Long = _numActualConnectionsCreated
  def numActiveConnections: Long = _numActiveConnections


  private[hbase] def incrementActualConnectionsCreated(incr: Long) = {
    _numActualConnectionsCreated += incr
  }

  private[hbase] def incrementTotalRequests(incr: Long) = {
    _numTotalRequests += incr
  }

  private[hbase] def setActiveConnections(numActiveConnections: Long) = {
    this._numActiveConnections = numActiveConnections
  }

  private[hbase] def copy(): HBaseConnectionCacheStat =
    HBaseConnectionCacheStat(numTotalRequests, numActualConnectionsCreated, numActiveConnections)

  // inplace update to reset - for tests
  private[hbase] def reset(): Unit = {
    _numTotalRequests = 0
    _numActualConnectionsCreated = 0
    _numActiveConnections = 0
  }
}

object HBaseConnectionCacheStat {
  def apply(numTotalRequests: Long,
      numActualConnectionsCreated: Long,
      numActiveConnections: Long): HBaseConnectionCacheStat =
    new HBaseConnectionCacheStat(numTotalRequests, numActualConnectionsCreated, numActiveConnections)

  def unapply(stat: HBaseConnectionCacheStat): Option[(Long, Long, Long)] =
    Some((stat.numTotalRequests, stat.numActualConnectionsCreated, stat.numActiveConnections))

}