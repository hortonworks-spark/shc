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

import scala.language.implicitConversions

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._

// Resource and ReferencedResources are defined for extensibility,
// e.g., consolidate scan and bulkGet in the future work.

// User has to invoke release explicitly to release the resource,
// and potentially parent resources
trait Resource {
  def release(): Unit
}

case class ScanResource(tbr: TableResource, rs: ResultScanner) extends Resource {
  def release() {
    rs.close()
    tbr.release()
  }
}

case class GetResource(tbr: TableResource, rs: Array[Result]) extends Resource {
  def release() {
    tbr.release()
  }
}

// Multiple child resource may use this one, which is reference counted.
// It will not be released until the counter reaches 0
trait ReferencedResource {
  var count: Int = 0
  def init(): Unit
  def destroy(): Unit
  def acquire() = synchronized {
    try {
      count += 1
      if (count == 1) {
        init()
      }
    } catch {
      case e: Throwable =>
        release()
        throw e
    }
  }

  def release() = synchronized {
    count -= 1
    if (count == 0) {
      destroy()
    }
  }

  def releaseOnException[T](func: => T): T = {
    acquire()
    val ret = {
      try {
        func
      } catch {
        case e: Throwable =>
          release()
          throw e
      }
    }
    ret
  }
}

case class RegionResource(relation: HBaseRelation) extends ReferencedResource {
  var connection: SmartConnection = _
  var rl: RegionLocator = _

  override def init(): Unit = {
    connection = HBaseConnectionCache.getConnection(relation.hbaseConf)
    rl = connection.getRegionLocator(TableName.valueOf(relation.catalog.namespace, relation.catalog.name))
  }

  override def destroy(): Unit = {
    if (rl != null) {
      rl.close()
      rl = null
    }
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  val regions = releaseOnException {
    val keys = rl.getStartEndKeys
    keys.getFirst.zip(keys.getSecond)
      .zipWithIndex
      .map(x =>
      HBaseRegion(x._2,
        Some(x._1._1),
        Some(x._1._2),
        Some(rl.getRegionLocation(x._1._1).getHostname)))
  }
}

case class TableResource(relation: HBaseRelation) extends ReferencedResource {
  var connection: SmartConnection = _
  var table: Table = _

  override def init(): Unit = {
    connection = HBaseConnectionCache.getConnection(relation.hbaseConf)
    table = connection.getTable(TableName.valueOf(relation.catalog.namespace, relation.catalog.name))
  }

  override def destroy(): Unit = {
    if (table != null) {
      table.close()
      table = null
    }
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  def get(list: java.util.List[org.apache.hadoop.hbase.client.Get]) = releaseOnException {
      GetResource(this, table.get(list))
  }

  def getScanner(scan: Scan): ScanResource = releaseOnException {
      ScanResource(this, table.getScanner(scan))
  }
}

object HBaseResources{
  implicit def ScanResToScan(sr: ScanResource): ResultScanner = {
    sr.rs
  }

  implicit def GetResToResult(gr: GetResource): Array[Result] = {
    gr.rs
  }

  implicit def TableResToTable(tr: TableResource): Table = {
    tr.table
  }

  implicit def RegionResToRegions(rr: RegionResource): Seq[HBaseRegion] = {
    rr.regions
  }
}
