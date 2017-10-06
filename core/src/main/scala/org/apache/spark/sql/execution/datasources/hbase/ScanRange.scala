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

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.annotation.tailrec

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.execution.datasources.hbase.types.SHCDataTypeFactory

case class Bound[T](point: T, inc: Boolean)(implicit ordering: Ordering[T]) {
  override def toString = {
    s"($point $inc)"
  }
  override def equals(that: Any): Boolean = that match {
    case b: Bound[T] =>
      ordering.compare(point, b.point) == 0 && inc == b.inc
    case _ => false
  }
}

// if optional is none, it starts/ends with minimum/maximum
case class ScanRange[T](start: Option[Bound[T]], end: Option[Bound[T]]) {
  def get(p: Option[Bound[T]]): Option[T] = {
    if (p.isDefined) {
      Some(p.get.point)
    } else {
      None
    }
  }
  override def toString = {
   // s"start: ${start.map(x=>Bytes.toString(x.point.asInstanceOf[Array[Byte]])).getOrElse("None")}
   // end: ${end.map(x=>Bytes.toString(x.point.asInstanceOf[Array[Byte]])).getOrElse("None")}"
    s"start: ${start.map(_.toString).getOrElse("None")} end: ${end.map(_.toString).getOrElse("None")}"
  }
}

object ScanRange {
  implicit val order: Ordering[Array[Byte]] =  hbase.ord
  def empty[T] = ScanRange[T](None, None)

  // split (a, b] to (a, b) and [b, b]
  def split[T](range: ScanRange[T])(implicit ordering: Ordering[T]): Seq[ScanRange[T]] = {
    if (range.end.map(_.inc).getOrElse(false) &&
      compare(range.start, range.end, ordering) != 0
    ) {
      Seq(ScanRange(range.start, Some(Bound(range.end.get.point, false))),
        ScanRange(
          Some(Bound(range.end.get.point, true)),
          Some(Bound(range.end.get.point, true))))
    } else {
      Seq(range)
    }
  }

  // For some type T, e.g., Bytes, there is no explicit maximum and minimum
  // If min is true, None = negative infinity
  // Otherwise, None = positive infinity
  def compare[T](
      b: Option[Bound[T]],
      point: Option[Bound[T]],
      ordering: Ordering[T],
      min: Boolean = true): Int = {
    if (b.isEmpty && point.isEmpty) {
      0
    } else if (b.isEmpty) {
      if (min) {
        -1
      } else {
        1
      }
    } else if (point.isEmpty) {
      if (min) {
        1
      } else {
        -1
      }
    } else {
      val tmp = ordering.compare(b.get.point, point.get.point)
      if (tmp == 0) {
        if (b.get.inc == point.get.inc) {
          0
        } else if (b.get.inc) {
          if (min) {
            // b =[a, point = (a
            -1
          } else {
            // b = a], point = a)
            1
          }
        } else {
          if (min) {
            // b = (a, point = [a
            1
          } else {
            // b = a), point = a]
            -1
          }
        }
      } else {
        tmp
      }
    }
  }

  // Return the first insertion point
  // If min is true, take None as Minimum
  // Otherwise, take None as Maximum
  private def bSearch[T](
      v: IndexedSeq[Option[Bound[T]]],
      point: Option[Bound[T]],
      ordering: Ordering[T],
      min: Boolean): Int = {
    var start = 0
    var end = v.length
    // invariant: all element left to start is less than k and all elements right to end
    // is no less than k
    while (start < end) {
      val mid = (start + end) >>> 1
      // equal is regarded as larger since we find the first insertion point
      val c = compare(v(mid), point, ordering, min)
      if (c >= 0) {
        end = mid
      } else {
        start = mid + 1
      }
    }
    start
  }

  def and[T](
      bounds: ScanRange[T],
      ranges: Array[ScanRange[T]])(implicit ordering: Ordering[T]): Array[ScanRange[T]] = {
    val search = new ArrayBuffer[ScanRange[T]]()
    // We assume that the range size is reasonably small, and no need to use binary search
    ranges.foreach { range =>
      val l =  {
        if (compare(bounds.start, range.start, ordering, true) >= 0) {
          bounds.start
        } else {
          range.start
        }
      }
      val r = {
        if (compare(bounds.end, range.end, ordering, false) <= 0) {
          bounds.end
        } else {
          range.end
        }
      }
      // Avoid l = (a, r = a) or l = [a, r = a) or l = (a, r = a]
      // We have to do this to avoid on executor to fetch any data from other region server
      if (l.isEmpty || r.isEmpty ||
        (ordering.compare(l.get.point, r.get.point) == 0 && l.get.inc && r.get.inc) ||
        ordering.compare(l.get.point, r.get.point) < 0) {
        search += (ScanRange(l, r))
      }
    }
    search.toArray
  }

  def or[T](
      extra: ScanRange[T],
      ranges: Array[ScanRange[T]])(implicit ordering: Ordering[T]): Array[ScanRange[T]] = {
    val search = new ArrayBuffer[ScanRange[T]]()
    val (l, u) = (ranges.map(_.start), ranges.map(_.end))
    val lIdx = bSearch(l, extra.start, ordering, true)
    val uIdx = bSearch(u, extra.end, ordering, false)
    val (low, lowIdx) = {
      // concatenate extra = (a, or [a with the other a] or a)
      // Do not fill the gap even it is only one element
      if (extra.start.isDefined && lIdx != 0) {
        if (u(lIdx -1).isDefined) {
          val tmp = ordering.compare(extra.start.get.point, u(lIdx - 1).get.point)
          if (tmp < 0 ||
            (tmp == 0 && (extra.start.get.inc || u(lIdx - 1).get.inc))) {
            (l(lIdx - 1), lIdx - 1)
          } else {
            (extra.start, lIdx)
          }
        } else {
          (l(lIdx - 1), lIdx - 1)
        }
      } else {
        (extra.start, lIdx)
      }
    }
    val (up, upIdx) = {
      // concatenate extra = a], or a) with the other (a or [a
      // there may be a gap filled but it is ok, the upper layer will discard it.
      if (uIdx != ranges.length) {
        if (extra.end.isDefined) {
          if (l(uIdx).isDefined) {
            val tmp = (ordering.compare(extra.end.get.point, l(uIdx).get.point))
            if (tmp > 0 ||
              (tmp == 0 && (extra.end.get.inc || l(uIdx).get.inc))) {
              // if larger than the lower bound, merge it with uIdx
              (u(uIdx), uIdx + 1)
            } else {
              // otherwise, keep the uIdx
              (extra.end, uIdx)
            }
          } else {
            (u(uIdx), uIdx + 1)
          }
        } else {
          // both extra and array has infinite at the end, merge it
          (u(uIdx), uIdx + 1)
        }
      } else {
        (extra.end, uIdx)
      }
    }
    (0 until lowIdx).foreach { x =>
      search.+=(ranges(x))
    }
    search.+= (ScanRange(low, up))
    (upIdx until ranges.length).foreach { x =>
      search.+= (ranges(x))
    }
    search.toArray
  }

  def and[T](
      left:  Array[ScanRange[T]],
      right:  Array[ScanRange[T]])(implicit ordering: Ordering[T]): Array[ScanRange[T]] = {
    // (0, 5), (10, 15) and with (2, 3) (8, 12) = (2, 3), (10, 12)
    val tmp = left.map(x => ScanRange.and(x, right))
    tmp.reduce[Array[ScanRange[T]]] { case (x, y) =>
      x.foldLeft(y) { case (m, n) =>
        ScanRange.or(n, m)
      }
    }
  }

  @tailrec def or[T](
      left:  Array[ScanRange[T]],
      right:  Array[ScanRange[T]])(implicit ordering: Ordering[T]): Array[ScanRange[T]] = {
    if(left.length <= right.length) {
      left.foldLeft(right){ case (x, y) =>
        ScanRange.or(y, x)
      }
    } else {
      or(right, left)
    }
  }

  // Construct multi-dimensional scan ranges.
  def apply(
      length: Int,
      low: Array[Byte],
      lowInc: Boolean,
      up: Array[Byte],
      upInc: Boolean,
      offset: Int): ScanRange[Array[Byte]] = {
    val end = Array.fill(length)(-1: Byte)
    System.arraycopy(up, 0, end, offset, up.length)
    ScanRange(Some(Bound(low, lowInc)), Some(Bound(end, upInc)))
  }
}

// Data type for range whose size is known.
// lower bound and upperbound for each range.
// If data order is the same as byte order, then left = mid = right.
// For the data type whose order is not the same as byte order, left != mid != right
// In this case, left is max, right is min and mid is the byte of the value.
// By this way, the scan will cover the whole range and will not  miss any data.
// Typically, mid is used only in Equal in which case, the order does not matter.
case class BoundRange(
    low: Array[Byte],
    upper: Array[Byte])

// The range in less and greater have to be lexi ordered.
case class BoundRanges(less: Array[BoundRange], greater: Array[BoundRange], value: Array[Byte])

object BoundRange extends Logging{

  def apply(in: Any, f: Field): Option[BoundRanges] = {
    val pt = SparkHBaseConf.PrimitiveType
    lazy val coder = SHCDataTypeFactory.create(f)
    lazy val b = coder.toBytes(in)

    in match {
      // For short, integer, and long, the order of number is consistent with byte array order
      // regardless of its sign. But the negative number is larger than positive number in byte array.
      case a: Integer =>
        if (f.fCoder == pt) {
          val b = Bytes.toBytes(a)
          if (a >= 0) {
            logDebug(s"range is 0 to $a and ${Integer.MIN_VALUE} to -1")
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(0: Int), b),
                BoundRange(Bytes.toBytes(Integer.MIN_VALUE), Bytes.toBytes(-1: Int))),
              Array(BoundRange(b, Bytes.toBytes(Integer.MAX_VALUE))), b))
          } else {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(Integer.MIN_VALUE), b)),
              Array(BoundRange(Bytes.toBytes(0: Int), Bytes.toBytes(Integer.MAX_VALUE)),
                BoundRange(b, Bytes.toBytes(-1: Integer))),
              b))
          }
        } else {
          // Non-PrimitiveType
          val min = coder.toBytes(Integer.MIN_VALUE)
          val max = coder.toBytes(Integer.MAX_VALUE)
          Some(BoundRanges(Array(BoundRange(min, b)),Array(BoundRange(b, max)), b))
        }

      case a: Long =>
        if (f.fCoder == pt) {
          val b =  Bytes.toBytes(a)
          if (a >= 0) {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(0: Long), b),
                BoundRange(Bytes.toBytes(Long.MinValue),  Bytes.toBytes(-1: Long))),
              Array(BoundRange(b,  Bytes.toBytes(Long.MaxValue))), b))
          } else {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(Long.MinValue), b)),
              Array(BoundRange(Bytes.toBytes(0: Long), Bytes.toBytes(Long.MaxValue)),
                BoundRange(b, Bytes.toBytes(-1: Long))), b))
          }
        } else {
          // Non-PrimitiveType
          val min = coder.toBytes(Long.MinValue)
          val max = coder.toBytes(Long.MaxValue)
          Some(BoundRanges(Array(BoundRange(min, b)),Array(BoundRange(b, max)), b))
        }

      case a: Short =>
        if (f.fCoder == pt) {
          val b =  Bytes.toBytes(a)
          if (a >= 0) {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(0: Short), b),
                BoundRange(Bytes.toBytes(Short.MinValue),  Bytes.toBytes(-1: Short))),
              Array(BoundRange(b,  Bytes.toBytes(Short.MaxValue))), b))
          } else {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(Short.MinValue), b)),
              Array(BoundRange(Bytes.toBytes(0: Short), Bytes.toBytes(Short.MaxValue)),
                BoundRange(b, Bytes.toBytes(-1: Short))), b))
          }
        } else {
          // Non-PrimitiveType
          val min = coder.toBytes(Short.MinValue)
          val max = coder.toBytes(Short.MaxValue)
          Some(BoundRanges(Array(BoundRange(min, b)),Array(BoundRange(b, max)), b))
        }

      case a: Double =>
        // For both double and float, the order of positive number is consistent
        // with byte array order. But the order of negative number is the reverse
        // order of byte array. Please refer to IEEE-754 and
        // https://en.wikipedia.org/wiki/Single-precision_floating-point_format
        if (f.fCoder == pt) {
          val b =  Bytes.toBytes(a)
          if (a >= 0.0f) {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(0.0d), b),
                BoundRange(Bytes.toBytes(-0.0d),  Bytes.toBytes(Double.MinValue))),
              Array(BoundRange(b,  Bytes.toBytes(Double.MaxValue))), b))
          } else {
            Some(BoundRanges(
              Array(BoundRange(b, Bytes.toBytes(Double.MinValue))),
              Array(BoundRange(Bytes.toBytes(0.0d), Bytes.toBytes(Double.MaxValue)),
                BoundRange(Bytes.toBytes(-0.0d), b)), b))
          }
        } else {
          // Non-PrimitiveType
          val min = coder.toBytes(Double.MinValue)
          val max = coder.toBytes(Double.MaxValue)
          Some(BoundRanges(Array(BoundRange(min, b)),Array(BoundRange(b, max)), b))
        }

      case a: Float =>
        if (f.fCoder == pt) {
          val b =  Bytes.toBytes(a)
          if (a >= 0.0f) {
            Some(BoundRanges(
              Array(BoundRange(Bytes.toBytes(0.0f), b),
                BoundRange(Bytes.toBytes(-0.0f),  Bytes.toBytes(Float.MinValue))),
              Array(BoundRange(b,  Bytes.toBytes(Float.MaxValue))), b))
          } else {
            Some(BoundRanges(
              Array(BoundRange(b, Bytes.toBytes(Float.MinValue))),
              Array( BoundRange(Bytes.toBytes(0.0f), Bytes.toBytes(Float.MaxValue)),
                BoundRange(Bytes.toBytes(-0.0f), b)), b))
          }
        } else {
          // Non-PrimitiveType
          val min = coder.toBytes(Float.MinValue)
          val max = coder.toBytes(Float.MaxValue)
          Some(BoundRanges(Array(BoundRange(min, b)),Array(BoundRange(b, max)), b))
        }

      case _: Array[Byte] | _: Byte | _: String | _: UTF8String =>
        Some(BoundRanges(
          Array(BoundRange(Array.fill(b.length)(ByteMin), b)),
          Array(BoundRange(b, Array.fill(b.length)(ByteMax))), b))

      case _ => None
    }
  }
}
