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

package org.apache.spark.sql.execution.datasources.hbase

import java.util.ArrayList

import org.apache.hadoop.hbase.filter.{Filter => HFilter, FilterList => HFilterList}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BinaryType, AtomicType}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.{CellUtil, Cell, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.apache.spark.{InterruptibleIterator, TaskContext, Partition, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter

private[hbase] case class HBaseRegions(
    override val index: Int,
    val start: Option[HBaseType] = None,
    val end: Option[HBaseType] = None,
    val server: Option[String] = None) extends Partition

private[hbase] case class HBaseScanPartition(
    override val index: Int,
    val regions: HBaseRegions,
    val scanRanges: Array[ScanRange[Array[Byte]]],
    val tf: SerializedHTypedFilter) extends Partition


private[hbase] class HBaseTableScanRDD(
    relation: HBaseRelation,
    requiredColumns: Array[String],
    filters: Array[Filter]) extends RDD[Row](relation.sqlContext.sparkContext, Nil) with Logging  {

  val columnFields = relation.splitRowKeyColumns(requiredColumns)._2

  override def getPartitions: Array[Partition] = {
    val hbaseFilter = HBaseFilter.buildFilters(filters, relation)
    val regions = relation.regions
    var idx = 0
    logInfo(s"There are ${regions.size} regions")
    regions.flatMap { x=>
      val pScan = ScanRange(Some(Bound(x.start.get, true)), Some(Bound(x.end.get, false)))
      val ranges = ScanRange.and(pScan, hbaseFilter.ranges)
      if (ranges.size > 0) {
        val p = Some(HBaseScanPartition(idx, x, ranges, HTypedFilter.toSerializedHTypedFilter(hbaseFilter.tf)))
        idx += 1
        p
      } else {
        None
      }
    }.toArray
  }

  def buildRow(
      indexedFields: Seq[(Field, Int)],
      result: Result,
      row: MutableRow) = {
    val r = result.getRow
    relation.catalog.dynSetupRowKey(r)
    indexedFields.map { x =>
      if (x._1.isRowKey) {
        if (x._1.start + x._1.length <= r.length) {
          Utils.setRowCol(row, x, r, x._1.start, x._1.length)
        } else {
          row.setNullAt(x._2)
        }
      } else {
        val kv = result.getColumnLatestCell(Bytes.toBytes(x._1.cf), Bytes.toBytes(x._1.col))
        if (kv == null || kv.getValueLength == 0) {
          row.setNullAt(x._2)
        } else if (x._1.dt.isInstanceOf[AtomicType]) {
          val v = CellUtil.cloneValue(kv)
          Utils.setRowCol(row, x, v, 0, v.length)
        }
      }
    }
  }

  private def toResultIterator(result: Array[Result]): Iterator[Result] = {
    val iterator = new Iterator[Result] {
      var idx = 0
      var cur: Option[Result] = None
      val stream = result.toStream
      override def hasNext: Boolean = {
        while(idx < result.length && cur.isEmpty) {
          val tmp = result(idx)
          idx += 1
          if (!tmp.isEmpty) {
            cur = Some(tmp)
          }
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }

  private def toResultIterator(scanner: ResultScanner): Iterator[Result] = {
    val iterator = new Iterator[Result] {
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        if (cur.isEmpty) {
          val r = scanner.next()
          if (r == null) {
            scanner.close()
          } else {
            cur = Some(r)
          }
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }

  private def toRowIterator(
      it: Iterator[Result]): Iterator[Row] = {

    val iterator = new Iterator[Row] {
      val row = new GenericMutableRow(requiredColumns.size)
      val indexedFields = relation.getIndexedProjections(requiredColumns)

      override def hasNext: Boolean = {
        it.hasNext
      }

      override def next(): Row = {
        val r = it.next()
        buildRow(indexedFields, r, row)
        row
      }
    }
    iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBaseScanPartition].regions.server.map {
      identity
    }.toSeq
  }

  private def buildScan(
      start: Option[HBaseType],
      end: Option[HBaseType],
      columns: Seq[Field], filter: Option[HFilter]): Scan = {
    val scan = {
      (start, end) match {
        case (Some(lb), Some(ub)) => new Scan(lb, ub)
        case (Some(lb), None) => new Scan(lb)
        case (None, Some(ub)) => new Scan(Array[Byte](), ub)
        case _ => new Scan
      }
    }
    // set fetch size
    // scan.setCaching(scannerFetchSize)
    columns.foreach{ c =>
      scan.addColumn(Bytes.toBytes(c.cf), Bytes.toBytes(c.col))
    }
    scan.setCaching(1000)
    if (filter.isDefined) {
      scan.setFilter(filter.get)
    }
    scan
  }

  private def buildGets(g: Array[ScanRange[Array[Byte]]], columns: Seq[Field]): Iterator[Result] = {
    g.grouped(SparkHBaseConf.BulkGetSize).flatMap{ x =>
      val gets = new ArrayList[Get]()
      x.foreach{ y =>
        val g = new Get(y.start.get.point)
        columns.foreach{ c =>
          g.addColumn(Bytes.toBytes(c.cf), Bytes.toBytes(c.col))
        }
        gets.add(g)
      }
      toResultIterator(relation.table.get(gets))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val ord = implicitly[Ordering[HBaseType]]
    val partition = split.asInstanceOf[HBaseScanPartition]
    // remove the inclusive upbound
    val scanRanges = partition.scanRanges.flatMap(ScanRange.split(_))
    val (g, s) = scanRanges.partition{x =>
      x.start.isDefined && x.end.isDefined && ScanRange.compare(x.start, x.end, ord) == 0
    }
    val gIt: Iterator[Result] = {
      if (g.isEmpty) {
        Iterator.empty: Iterator[Result]
      } else {
        buildGets(g, columnFields)
      }
    }
    val scans = s.map(x =>
      buildScan(x.get(x.start), x.get(x.end), columnFields,
        HTypedFilter.fromSerializedHTypedFilter(partition.tf).filter))

    val sIts = scans.par.map(relation.table.getScanner(_)).map(toResultIterator(_))

    val rIt = sIts.fold(Iterator.empty: Iterator[Result]){ case (x, y) =>
      x ++ y
    } ++ gIt

    toRowIterator(rIt)
  }
}
