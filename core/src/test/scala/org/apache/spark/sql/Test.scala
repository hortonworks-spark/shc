/*
 * (C) 2017 Hortonworks, Inc. All rights reserved. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. This file is licensed to You under the Apache License, Version 2.0
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

package org.apache.spark.sql

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types.BinaryType

object Test {
  def main(args: Array[String]) {
    val a: Array[Byte] = Array.fill(10)(Byte.MinValue)
    val b = Bytes.toBytes ("row003")
    System.arraycopy(b, 0, a, 0, b.length)
    val c = Bytes.toBytes(Int.MinValue)
    System.arraycopy(c, 0, a, b.length, c.length)
    val len = a.indexOf(HBaseTableCatalog.delimiter, 0)
    val s1 = Bytes.toString(a, 0, 6)
    val s2 = Bytes.toString(a, 0, len)

    /*val l = Bytes.toBytes(Float.MinValue)
    val m = Bytes.toBytes(-20.0.asInstanceOf[Double])
    val n = Bytes.toBytes(0.0.asInstanceOf[Double])
    val o = Bytes.toBytes(20.0.asInstanceOf[Double])
    val p = Bytes.toBytes(Float.MaxValue)*/
    val l = Array.fill(8)(Byte.MaxValue)
    Bytes.putDouble(l, 0, Double.MinValue)
    val m = Array.fill(8)(Byte.MaxValue)
    Bytes.putDouble(m, 0, -20.0)
    val n = Array.fill(8)(Byte.MaxValue)
    Bytes.putDouble(n, 0, 0.0)
    val o = Array.fill(8)(Byte.MaxValue)
    Bytes.putDouble(o,  0, 20.0)
    val p = Array.fill(8)(Byte.MaxValue)
    Bytes.putDouble(p, 0, Double.MaxValue)

    val c1 = BinaryType.ordering.compare(l, m)
    val c2 = BinaryType.ordering.compare(m, n)
    val c3 = BinaryType.ordering.compare(n, o)
    val c4 = BinaryType.ordering.compare(o, p)

    val p1 = Array.fill(10)(0: Byte)
    Bytes.putBytes(p1, 0, Bytes.toBytes("row010"), 0, 6)

    val p2 = Array.fill(10)(-1: Byte)
    Bytes.putBytes(p2, 0, Bytes.toBytes("row010"), 0, 6)

    val p3 = Array.fill(10)(Byte.MaxValue)
    Bytes.putBytes(p3, 0, Bytes.toBytes("row010"), 0, 6)
    Bytes.putInt(p3, 6, 10)

    val p4 = Bytes.compareTo(p1, p3)
    val p5 = Bytes.compareTo(p2, p3)

    val z = Array.fill(4)(Byte.MinValue)
    Bytes.putInt(z, 0, -1)
    val z1 = Array.fill(4)(Byte.MinValue)
    Bytes.putInt(z1, 0, -2147483648)

    val z2 = Bytes.compareTo(z, z1)

    val t = Array.fill(4)(-1: Byte)
    println(Bytes.toInt(t))

    val s = Bytes.toBytes(1.4.asInstanceOf[Float])
    println(Bytes.toInt(s))
    println(Bytes.toFloat(s))
    val w =  Bytes.toBytes(-1.4.asInstanceOf[Float])
    println(Bytes.toInt(w))
    println(Bytes.toFloat(w))

    val buffer1 = Bytes.toBytes(-1.0f)
    val b1 = Bytes.toInt(buffer1)
    var buffer = Array.fill(4)(-1: Byte)
    var buffer2 = Bytes.toBytes(-1.0f)

    var buffer3 = java.lang.Float.floatToIntBits(-1.0f)
    val b3 = Bytes.toBytes(buffer3)
    val out = Bytes.toInt(buffer1) ^ Integer.MIN_VALUE
    buffer2 = Bytes.toBytes(out)
    var i: Int = java.lang.Float.floatToIntBits(-1.0f)
    i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1
    Bytes.putInt(buffer, 0, i)

    val mn = Bytes.toBytes(-0.0f)
    println(Bytes.toFloat(mn))
    println(Float.MinPositiveValue)

    println(s"a")
  }
}
