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
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase.{Bound, ScanRange}
import org.apache.spark.sql.types.BinaryType

class ScanRangeTestSuite  extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging {

  implicit val order =  BinaryType.ordering
  var num = 1

  def display() {
    logInfo(s"test case $num:")
    num += 1
  }
  test("Test andRange 1") {
    val v = ScanRange.and(ScanRange(Some(Bound(50, true)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(52, false))),
        ScanRange(Some(Bound(80, true)), Some(Bound(120, false)))))
    display
    v.foreach(x => logDebug(x.toString))
    val ret = Set(ScanRange(Some(Bound(50, true)), Some(Bound(52, false))),
      ScanRange(Some(Bound(80, true)), Some(Bound(100, false)))
    )
    assert(v.toSet == ret)
  }
  test("Test andRange 2") {
    val v = ScanRange.and(ScanRange(Some(Bound(50, false)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
        ScanRange(Some(Bound(80, false)), Some(Bound(120, false)))))

    display
    v.foreach(x => logDebug(x.toString))
    val ret = Set(ScanRange(Some(Bound(50, false)), Some(Bound(52, true))),
      ScanRange(Some(Bound(80, false)), Some(Bound(100, false)))
    )
    assert(v.toSet == ret)
  }
  test("Test andRange 3") {
    val v = ScanRange.and(ScanRange(None, Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
        ScanRange(Some(Bound(80, false)), Some(Bound(120, false)))))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
      ScanRange(Some(Bound(80, false)), Some(Bound(100, false)))
    )
    assert(v.toSet == ret)
  }

  test("Test andRange 4") {
    val v = ScanRange.and(ScanRange[Int](None, None),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
        ScanRange(Some(Bound(80, false)), Some(Bound(120, false)))))

    display
    v.foreach(x => logDebug(x.toString))
    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
      ScanRange(Some(Bound(80, false)), Some(Bound(120, false)))
    )
    assert(v.toSet == ret)
  }

  test("Test andRange 5") {
    val v = ScanRange.and(ScanRange[Int](None, None),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
        ScanRange(Some(Bound(80, false)), None)))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(52, true))),
      ScanRange(Some(Bound(80, false)), None)
    )
    assert(v.toSet == ret)
  }

  test("Test andRange 6") {
    val v = ScanRange.and(ScanRange[Int](None, None),
      Array(ScanRange(None, Some(Bound(52, true))),
        ScanRange(Some(Bound(80, false)), None)))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(None, Some(Bound(52, true))),
      ScanRange(Some(Bound(80, false)), None)
    )
    assert(v.toSet == ret)
  }

  test("Test andRange 7") {
    val v = ScanRange.and(ScanRange(Some(Bound(50, true)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), Some(Bound(120, false)))))
    display
    v.foreach(x => logDebug(x.toString))
    val ret = Set.empty
    assert(v.toSet == ret)
  }

  test("Test andRange 8") {
    val v = ScanRange.and(ScanRange(Some(Bound(50, true)), Some(Bound(50, true))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(50, false)), None)))
    display
    v.foreach(x => logDebug(x.toString))
    val ret = Set.empty
    assert(v.toSet == ret)
  }

  test("Test andRange 9") {
    val v = ScanRange.and(ScanRange(Some(Bound(50, true)), Some(Bound(50, true))),
      Array(ScanRange[Int](None, None)))
    display
    v.foreach(x => logDebug(x.toString))
    val ret = Set(ScanRange(Some(Bound(50, true)), Some(Bound(50, true))))
    assert(v.toSet == ret)
  }

  test("Test andRange 19") {
    val v = ScanRange.and(ScanRange[Int](None, None),
      Array(ScanRange[Int](None, None)))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange[Int](None, None))

    assert(v.size == ret.size && v.toSet == ret)
  }

  test("Test andRange 20") {
    val v = ScanRange.and(
      Array(ScanRange(Some(Bound(10, true)), Some(Bound(20, true))),
        ScanRange(Some(Bound(30, true)), Some(Bound(40, true)))),
      Array(ScanRange(Some(Bound(5, true)), Some(Bound(15, false))),
        ScanRange(Some(Bound(35, true)), Some(Bound(45, false)))))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(10, true)), Some(Bound(15, false))),
      ScanRange(Some(Bound(35, true)), Some(Bound(40, true))))

    assert(v.size == ret.size && v.toSet == ret)
  }

  test("Test andRange 21") {
    val v = ScanRange.and(
      Array(ScanRange(Some(Bound(5, true)), Some(Bound(15, false)))),
      Array(ScanRange[Int](None, None)))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(5, true)), Some(Bound(15, false))))

    assert(v.size == ret.size && v.toSet == ret)
  }

  test("Test orRange 1") {
    val v = ScanRange.or(ScanRange(Some(Bound(50, true)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(60, false))),
        ScanRange(Some(Bound(80, true)), Some(Bound(120, false))),
        ScanRange(Some(Bound(150, true)), Some(Bound(200, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(120, false))),
      ScanRange(Some(Bound(150, true)), Some(Bound(200, false))))

    assert(v.toSet == ret)
  }

  test("Test orRange 2") {
    val v = ScanRange.or(ScanRange(Some(Bound(50, true)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), Some(Bound(120, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(120, false))))

    assert(v.toSet == ret)
  }

  test("Test orRange 3") {
    val v = ScanRange.or(ScanRange(Some(Bound(50, false)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), Some(Bound(120, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
      ScanRange(Some(Bound(50, false)), Some(Bound(120, false))))
    assert(v.toSet == ret)
  }

  test("Test orRange 4") {
    val v = ScanRange.or(ScanRange(None, Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), Some(Bound(120, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(None, Some(Bound(120, false))))
    assert(v.toSet == ret)
  }

  test("Test orRange 5") {
    val v = ScanRange.or(ScanRange(None, Some(Bound(100, false))),
      Array(ScanRange(None, Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), Some(Bound(120, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(None, Some(Bound(120, false))))
    assert(v.toSet == ret)
  }


  test("Test orRange 6") {
    val v = ScanRange.or(ScanRange(Some(Bound(0, true)), Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), None)))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), None))
    assert(v.toSet == ret)
  }


  test("Test orRange 7") {
    val v = ScanRange.or(ScanRange(None, Some(Bound(100, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(50, false))),
        ScanRange(Some(Bound(100, true)), None)))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(None, None))
    assert(v.toSet == ret)
  }

  test("Test orRange 8") {
    val v = ScanRange.or(ScanRange(Some(Bound(50, true)), Some(Bound(150, false))),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(60, false))),
        ScanRange(Some(Bound(80, true)), Some(Bound(120, false))),
        ScanRange(Some(Bound(150, true)), Some(Bound(200, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(200, false))))

    assert(v.toSet == ret)
  }

  test("Test orRange 9") {
    val v = ScanRange.or(ScanRange(Some(Bound(70, true)), None),
      Array(ScanRange(Some(Bound(0, true)), Some(Bound(60, false))),
        ScanRange(Some(Bound(80, true)), Some(Bound(120, false))),
        ScanRange(Some(Bound(150, true)), Some(Bound(200, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(0, true)), Some(Bound(60, false))),
      ScanRange(Some(Bound(70, true)), None))

    assert(v.toSet == ret)
  }

  test("Test orRange 10") {
    val v = ScanRange.or(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      Array(ScanRange(Some(Bound(71, true)), Some(Bound(71, true)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      ScanRange(Some(Bound(71, true)), Some(Bound(71, true))))

    assert(v.toSet == ret)
  }

  test("Test orRange 11") {
    val v = ScanRange.or(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      Array(ScanRange(Some(Bound(70, true)), Some(Bound(71, true)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(70, true)), Some(Bound(71, true))))

    assert(v.toSet == ret)
  }

  test("Test orRange 12") {
    val v = ScanRange.or(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      Array(ScanRange(Some(Bound(70, false)), Some(Bound(71, true)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(70, true)), Some(Bound(71, true))))

    assert(v.toSet == ret)
  }

  test("Test orRange 13") {
    val v = ScanRange.or(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      Array(ScanRange(None, Some(Bound(70, false)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(None, Some(Bound(70, true))))

    assert(v.toSet == ret)
  }

  test("Test orRange 14") {
    val v = ScanRange.or(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      Array(ScanRange(Some(Bound(70, false)), None)))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(70, true)), None))

    assert(v.toSet == ret)
  }

  test("Test orRange 15") {
    val v = ScanRange.or(ScanRange(Some(Bound(80, false)), Some(Bound(90, false))),
      Array(ScanRange(Some(Bound(70, true)), Some(Bound(70, true)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(70, true)), Some(Bound(70, true))),
      ScanRange(Some(Bound(80, false)), Some(Bound(90, false))))

    assert(v.toSet == ret)
  }

  test("Test orRange 16") {
    val v = ScanRange.or(ScanRange(Some(Bound(80, false)), Some(Bound(90, false))),
      Array(ScanRange(Some(Bound(100, true)), Some(Bound(100, true)))))
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(100, true)), Some(Bound(100, true))),
      ScanRange(Some(Bound(80, false)), Some(Bound(90, false))))

    assert(v.toSet == ret)
  }

  test("Test orRange 17") {
    val tmp =       Array(
      ScanRange(Some(Bound("r20", true)), Some(Bound("r20", true))),
      ScanRange(Some(Bound("row005", true)), Some(Bound("row005", true))),
      ScanRange(Some(Bound("row020", true)), Some(Bound("row020", true))),
      ScanRange(Some(Bound("row040", false)), Some(Bound("row050", true)))
    )
    val v = ScanRange.or(ScanRange(None, Some(Bound("row005", true))),tmp)
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(
      ScanRange(None, Some(Bound("row005", true))),
      ScanRange(Some(Bound("row020", true)), Some(Bound("row020", true))),
      ScanRange(Some(Bound("row040", false)), Some(Bound("row050", true)))
    )

    assert(v.toSet == ret)
  }

  test("Test orRange 18") {
    val tmp =       Array(
      ScanRange(Some(Bound(Bytes.toBytes(s"r20"), true)), Some(Bound(Bytes.toBytes(s"r20"), true))),
      ScanRange(Some(Bound(Bytes.toBytes(s"row005"), true)), Some(Bound(Bytes.toBytes(s"row005"), true))),
      ScanRange(Some(Bound(Bytes.toBytes(s"row020"), true)), Some(Bound(Bytes.toBytes(s"row020"), true))),
      ScanRange(Some(Bound(Bytes.toBytes(s"row040"), false)), Some(Bound(Bytes.toBytes(s"row050"), true)))
    )
    val v = ScanRange.or(ScanRange(None, Some(Bound(Bytes.toBytes(s"row005"), true))),tmp)
    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(
      ScanRange(None, Some(Bound(Bytes.toBytes(s"row005"), true))),
      ScanRange(Some(Bound(Bytes.toBytes(s"row020"), true)), Some(Bound(Bytes.toBytes(s"row020"), true))),
      ScanRange(Some(Bound(Bytes.toBytes(s"row040"), false)), Some(Bound(Bytes.toBytes(s"row050"), true)))
    )

    assert(v.toSet == ret)
  }

  test("Test orRange 19") {
    val v = ScanRange.or(ScanRange(Some(Bound(10, true)), Some(Bound(20, true))),
      Array(ScanRange(Some(Bound(10, true)), Some(Bound(20, true)))))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(10, true)), Some(Bound(20, true))))

    assert(v.size == ret.size && v.toSet == ret)
  }



  test("Test orRange 20") {
    val v = ScanRange.or(ScanRange[Int](None, None),
      Array(ScanRange[Int](None, None)))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange[Int](None, None))

    assert(v.size == ret.size && v.toSet == ret)
  }

  test("Test orRange 21") {
    val v = ScanRange.or(ScanRange(Some(Bound(10, true)), Some(Bound(20, true))),
      Array(ScanRange(Some(Bound(10, true)), Some(Bound(20, false)))))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(10, true)), Some(Bound(20, true))))

    assert(v.size == ret.size && v.toSet == ret)
  }

  test("Test orRange 22") {
    val v = ScanRange.or(
      Array(ScanRange(Some(Bound(10, true)), Some(Bound(20, true))),
        ScanRange(Some(Bound(30, true)), Some(Bound(40, true)))),
      Array(ScanRange(Some(Bound(5, true)), Some(Bound(15, false))),
        ScanRange(Some(Bound(35, true)), Some(Bound(45, false)))))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange(Some(Bound(5, true)), Some(Bound(20, true))),
      ScanRange(Some(Bound(30, true)), Some(Bound(45, false))))

    assert(v.size == ret.size && v.toSet == ret)
  }

  test("Test orRange 23") {
    implicit val order: Ordering[Array[Byte]] =  hbase.ord//BinaryType.ordering
    val v = ScanRange.or(
      Array(ScanRange[Array[Byte]](Some(Bound[Array[Byte]](Bytes.toBytes("row005"), true)),
        Some(Bound[Array[Byte]](Bytes.toBytes("row005"), true)))),
      Array(ScanRange[Array[Byte]](Some(Bound[Array[Byte]](Array.fill(6)(0: Byte), true)),
        Some(Bound[Array[Byte]](Bytes.toBytes("row005"), true))),
        ScanRange[Array[Byte]](Some(Bound(Array.fill(6)(Byte.MinValue), true)),
          Some(Bound[Array[Byte]](Array.fill(6)(-1: Byte), true)))))

    display
    v.foreach(x => logDebug(x.toString))

    val ret = Set(ScanRange[Array[Byte]](Some(Bound[Array[Byte]](Array.fill(6)(0: Byte), true)),
      Some(Bound[Array[Byte]](Bytes.toBytes("row005"), true))),
      ScanRange[Array[Byte]](Some(Bound[Array[Byte]](Array.fill(6)(Byte.MinValue), true)),
        Some(Bound[Array[Byte]](Array.fill(6)(-1: Byte), true))))

    assert(v.size == ret.size && v.toSet == ret)
  }
}
