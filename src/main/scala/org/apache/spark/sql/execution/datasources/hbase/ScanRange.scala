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

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering

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
    s"start: ${start.map(_.toString).getOrElse("None")} end: ${end.map(_.toString).getOrElse("None")}"
  }
}

object ScanRange {
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
    }else if (point.isEmpty) {
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

  def or[T](
             left:  Array[ScanRange[T]],
             right:  Array[ScanRange[T]])(implicit ordering: Ordering[T]): Array[ScanRange[T]] = {
    left.foldLeft(right){ case (x, y) =>
      ScanRange.or(y, x)
    }
  }
}
