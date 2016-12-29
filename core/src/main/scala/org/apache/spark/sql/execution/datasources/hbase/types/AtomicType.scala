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

package org.apache.spark.sql.execution.datasources.hbase.types

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.execution.datasources.hbase._

class AtomicType(f: Field,
                 offset: Option[Int] = Some(0),
                 length: Option[Int] = None) extends SHCDataType {

  def toObject(src: HBaseType): Any = {
    val len = length.getOrElse(f.length)
    f.dt match {
      case BooleanType => toBoolean(src, offset.get)
      case ByteType => src(offset.get)
      case DoubleType => Bytes.toDouble(src, offset.get)
      case FloatType => Bytes.toFloat(src, offset.get)
      case IntegerType => Bytes.toInt(src, offset.get)
      case LongType => Bytes.toLong(src, offset.get)
      case ShortType => Bytes.toShort(src, offset.get)
      case StringType => toUTF8String(src, offset.get, len)
      case BinaryType =>
        val newArray = new Array[Byte](len)
        System.arraycopy(src, offset.get, newArray, 0, len)
        newArray
      case _ => SparkSqlSerializer.deserialize[Any](src) //TODO
    }
  }

  def toBytes(input: Any): Array[Byte] = {
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
      case _ => throw new Exception(s"unsupported data type ${f.dt}") //TODO
    }
  }

  def toBoolean(input: HBaseType, offset: Int): Boolean = {
    input(offset) != 0
  }

  def toUTF8String(input: HBaseType, offset: Int, length: Int): UTF8String = {
    UTF8String.fromBytes(input.slice(offset, offset + length))
  }
}
