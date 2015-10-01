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
import java.util.Comparator

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering


/**
 * Created by zzhang on 8/18/15.
 */
object Utils {

  def setRowCol(
      row: MutableRow,
      field: (Field, Int),
      src: HBaseType,
      offset: Int,
      length: Int): Unit = {
    val index = field._2
    if (field._1.sedes.isDefined) {
      // If we already have sedes defined , use it.
      val m = field._1.sedes.get.deserialize(src, offset, length)
      row.update(index, m)
    } else if (field._1.schema.isDefined) {
      // If we have avro schema defined, use it to get record, and then covert them to catalyst data type
      val m = AvroSedes.deserialize(src, field._1.schema.get)
      val n = field._1.avroToCatalyst.map(_(m))
      row.update(index, n)
    } else  {
      // Fall back to atomic type
      field._1.dt match {
        case BooleanType => row.setBoolean(index, toBoolean(src, offset))
        case ByteType => row.setByte(index, src(offset))
        case DoubleType => row.setDouble(index, Bytes.toDouble(src, offset))
        case FloatType => row.setFloat(index, Bytes.toFloat(src, offset))
        case IntegerType => row.setInt(index, Bytes.toInt(src, offset))
        case LongType => row.setLong(index, Bytes.toLong(src, offset))
        case ShortType => row.setShort(index, Bytes.toShort(src, offset))
        case StringType => row.update(index, toUTF8String(src, offset, length))
        case BinaryType =>
          val newArray = new Array[Byte](length)
          System.arraycopy(src, offset, newArray, 0, length)
          row.update(index, newArray)
        case _ => row.update(index, SparkSqlSerializer.deserialize[Any](src)) //TODO
      }
    }
  }

  // convert input to data type
  def toBytes(input: Any, field: Field): Array[Byte] = {
    if (field.sedes.isDefined) {
      field.sedes.get.serialize(input)
    } else if (field.schema.isDefined) {
      // Here we assume the top level type is structType
      val record =field.catalystToAvro(field.colName, "recordNamespace")(input)

      AvroSedes.serialize(record, field.schema.get)
    } else {
      input match {
        case data: Boolean => Bytes.toBytes(data)
        case data: Byte => Array(data)
        case data: Array[Byte] => data
        case data: Double => Bytes.toBytes(data)
        case data: Float => Bytes.toBytes(data)
        case data: Int => Bytes.toBytes(data)
        case data: Long => Bytes.toBytes(data)
        case data: Short => Bytes.toBytes(data)
        case data: UTF8String => data.getBytes
        case data: String => Bytes.toBytes(data)
          //Bytes.toBytes(input.asInstanceOf[String])//input.asInstanceOf[UTF8String].getBytes
        case _ => throw new Exception(s"unsupported data type ${field.dt}") //TODO
      }
    }

  }

  def toBoolean(input: HBaseType, offset: Int): Boolean = {
    input(offset) != 0
  }

  def toByte(input: HBaseType, offset: Int): Byte = {
    // Flip sign bit back
    val v: Int = input(offset) ^ 0x80
    v.asInstanceOf[Byte]
  }

  def toDouble(input: HBaseType, offset: Int): Double = {
    var l: Long = Bytes.toLong(input, offset, Bytes.SIZEOF_DOUBLE)
    l = l - 1
    l ^= (~l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE
    java.lang.Double.longBitsToDouble(l)
  }
  def toFloat(input: HBaseType, offset: Int): Float = {
    var i = Bytes.toInt(input, offset)
    i = i - 1
    i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE
    java.lang.Float.intBitsToFloat(i)
  }

  def toInt(input: HBaseType, offset: Int): Int = {
    // Flip sign bit back
    var v: Int = input(offset) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_INT - 1) {
      v = (v << 8) + (input(i + offset) & 0xff)
    }
    v
  }

  def toLong(input: HBaseType, offset: Int): Long = {
    // Flip sign bit back
    var v: Long = input(offset) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_LONG - 1) {
      v = (v << 8) + (input(i + offset) & 0xff)
    }
    v
  }

  def toShort(input: HBaseType, offset: Int): Short = {
    // flip sign bit back
    var v: Int = input(offset) ^ 0x80
    v = (v << 8) + (input(1 + offset) & 0xff)
    v.asInstanceOf[Short]
  }

  def toUTF8String(input: HBaseType, offset: Int, length: Int): UTF8String = {
    UTF8String(input.slice(offset, offset + length))
  }


}
