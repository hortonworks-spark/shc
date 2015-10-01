package org.apache.spark.sql

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types.BinaryType

/**
 * Created by zzhang on 9/23/15.
 */
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
    println(s"a")
  }
}
