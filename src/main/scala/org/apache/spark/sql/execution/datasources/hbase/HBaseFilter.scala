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

import java.util

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter => HFilter, FilterList => HFilterList, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase.HFilterType.HFilterType
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.BinaryType
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Created by zzhang on 9/2/15.
 */

object HFilterType extends Enumeration {
  type HFilterType = Value
  val And, Or, Atomic, Prefix, Und = Value
  def getOperator(hType: HFilterType): HFilterList.Operator = hType match {
    case And => HFilterList.Operator.MUST_PASS_ALL
    case Or => HFilterList.Operator.MUST_PASS_ONE
  }
  def parseFrom(fType: HFilterType): Array[Byte] => HFilter = fType match {
    case And | Or => {
      x: Array[Byte] => HFilterList.parseFrom(x).asInstanceOf[HFilter]
    }
    case Atomic  => {
      x: Array[Byte] => SingleColumnValueFilter.parseFrom(x).asInstanceOf[HFilter]
    }
    case Prefix =>  {
      x: Array[Byte] => PrefixFilter.parseFrom(x).asInstanceOf[HFilter]
    }
    case Und => throw new Exception("unknown type")
  }
}

case class HTypedFilter(filter: Option[HFilter], hType: HFilterType)

case class SerializedHTypedFilter(b: Option[Array[Byte]], hType: HFilterType)

object HTypedFilter {
  def toSerializedHTypedFilter(tf: HTypedFilter): SerializedHTypedFilter = {
    val b = tf.filter.map(_.toByteArray)
    SerializedHTypedFilter(b, tf.hType)
  }

  def fromSerializedHTypedFilter(tf: SerializedHTypedFilter): HTypedFilter = {
    val filter = tf.b.map(x => HFilterType.parseFrom(tf.hType)(x))
    HTypedFilter(filter, tf.hType)
  }

  def empty = HTypedFilter(None, HFilterType.Und)

  private def getOne(left: HTypedFilter, right: HTypedFilter) = {
    if (left.filter.isEmpty) {
      right
    } else {
      left
    }
  }

  private def ops(left: HTypedFilter, right: HTypedFilter, hType: HFilterType) = {
    if (left.hType == hType) {
      val l = left.filter.get.asInstanceOf[HFilterList]
      if (right.hType == hType) {
        val r = right.filter.get.asInstanceOf[HFilterList].getFilters
        r.foreach(l.addFilter(_))
      } else {
        l.addFilter(right.filter.get)
      }
      left
    } else if (right.hType == hType) {
      val r = right.filter.get.asInstanceOf[HFilterList]
      r.addFilter(left.filter.get)
      right
    } else {
      val nf = new HFilterList(HFilterType.getOperator(hType))
      nf.addFilter(left.filter.get)
      nf.addFilter(right.filter.get)
      HTypedFilter(Some(nf), hType)
    }
  }

  def and(left: HTypedFilter, right: HTypedFilter): HTypedFilter = {
    if (left.filter.isEmpty) {
      right
    } else if (right.filter.isEmpty) {
      left
    } else {
      ops(left, right, HFilterType.And)
    }
  }
  def or(left: HTypedFilter, right: HTypedFilter): HTypedFilter = {
    if (left.filter.isEmpty || right.filter.isEmpty) {
      HTypedFilter.empty
    } else {
      ops(left, right, HFilterType.Or)
    }
  }
}

// Combination of HBase range and filters
case class HRF[T](ranges: Array[ScanRange[T]], tf: HTypedFilter)

object HRF {
  def empty[T] = HRF[T](Array(ScanRange.empty[T]), HTypedFilter.empty)
}

object HBaseFilter {

  def buildFilters(filters: Array[Filter], relation: HBaseRelation): HRF[Array[Byte]] = {
    filters.foldLeft(HRF.empty[Array[Byte]]) { case (x, y) =>
        and[Array[Byte]](x, buildFilter(y, relation))
    }
  }

  private def toBytes[T](value: T, att: String, relation: HBaseRelation): Array[Byte] = {
    Utils.toBytes(value, relation.getField(att))
  }


  def buildFilter(filter: Filter, relation: HBaseRelation): HRF[Array[Byte]] = {
    val f = filter match {
      case And(left, right) =>
        and[Array[Byte]](buildFilter(left, relation), buildFilter(right, relation))
      case Or(left, right) =>
        or[Array[Byte]](buildFilter(left, relation), buildFilter(right, relation))
      case EqualTo(attribute, value) =>
        val b = toBytes(value, attribute, relation)
        if (relation.isPrimaryKey(attribute)) {
          HRF(Array(ScanRange(Some(Bound(b, true)), Some(Bound(b, true)))), HTypedFilter.empty)
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.EQUAL,
            b
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }
      case LessThan(attribute, value) =>
        val b = toBytes(value, attribute, relation)
        if (relation.isPrimaryKey(attribute)) {
          HRF(Array(ScanRange(None, Some(Bound(b, false)))), HTypedFilter.empty)
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.LESS,
            b
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }
      case LessThanOrEqual(attribute, value)  =>
        val b = toBytes(value, attribute, relation)
        if (relation.isPrimaryKey(attribute)) {
          HRF(Array(ScanRange(None, Some(Bound(b, true)))), HTypedFilter.empty)
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.LESS_OR_EQUAL,
            b
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }

      case GreaterThan(attribute, value) =>
        val b = toBytes(value, attribute, relation)

        if (relation.isPrimaryKey(attribute)) {
          HRF(Array(ScanRange(Some(Bound(b, false)), None)), HTypedFilter.empty)
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.GREATER,
            b
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }

      case GreaterThanOrEqual(attribute, value)  =>
        val b = toBytes(value, attribute, relation)
        if (relation.isPrimaryKey(attribute)) {
          HRF(Array(ScanRange(Some(Bound(b, true)), None)), HTypedFilter.empty)
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.GREATER_OR_EQUAL,
            b
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }

      case  StringStartsWith(attribute, value)  =>
        val b = Bytes.toBytes(value)
        if (relation.isPrimaryKey(attribute)) {
          val prefixFilter = new PrefixFilter(b)
          HRF(Array(ScanRange.empty[Array[Byte]]),
            HTypedFilter(Some(prefixFilter), HFilterType.Prefix))
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.EQUAL,
            new BinaryPrefixComparator(b)
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }

      case  StringEndsWith(attribute, value) if (relation.isColumn(attribute)) =>
        val f = relation.getField(attribute)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(f.cf),
          Bytes.toBytes(f.col),
          CompareOp.EQUAL,
          new RegexStringComparator(s".*$value")
        )
        HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))

      case StringContains(attribute: String, value: String) if relation.isColumn(attribute) =>
        val f = relation.getField(attribute)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(f.cf),
          Bytes.toBytes(f.col),
          CompareOp.EQUAL,
          new SubstringComparator(value)
        )
        HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))
      // We shoudl also add Not(GreatThan, LessThan, ...)
      // because if we miss some filter, it may result in a big overhead.
      case Not(StringContains(attribute: String, value: String)) if relation.isColumn(attribute) =>
        val b = Bytes.toBytes(value)
        val f = relation.getField(attribute)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(f.cf),
          Bytes.toBytes(f.col),
          CompareOp.NOT_EQUAL,
          new SubstringComparator(value)
        )
        HRF(Array(ScanRange.empty[Array[Byte]]), HTypedFilter(Some(filter), HFilterType.Atomic))

      case _ => HRF.empty[Array[Byte]]
    }

    println(s"start filter $filter")
    f.ranges.foreach(println(_))
    f
  }

  def and[T](
      left: HRF[T],
      right: HRF[T])(implicit ordering: Ordering[T]):HRF[T] = {
    // (0, 5), (10, 15) and with (2, 3) (8, 12) = (2, 3), (10, 12)
    val tmp = left.ranges.map(x => ScanRange.and(x, right.ranges))
    val ranges = ScanRange.and(left.ranges, right.ranges)
    val typeFilter = HTypedFilter.and(left.tf, right.tf)
    HRF(ranges, typeFilter)
  }

  def or[T](
      left: HRF[T],
      right: HRF[T])(implicit ordering: Ordering[T]):HRF[T] = {
    val ranges = ScanRange.or(left.ranges, right.ranges)
    val typeFilter = HTypedFilter.or(left.tf, right.tf)
    HRF(ranges, HTypedFilter.empty)
  }
}
