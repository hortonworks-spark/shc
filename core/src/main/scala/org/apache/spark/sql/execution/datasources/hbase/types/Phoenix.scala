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

package org.apache.spark.sql.execution.datasources.hbase.types

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.phoenix.query.QueryConstants
import org.apache.phoenix.schema._
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder
import org.apache.phoenix.schema.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.types._

class Phoenix(f:Option[Field] = None) extends SHCDataType {
  private var schema: RowKeySchema = null

  def fromBytes(src: HBaseType): Any = {
    if (f.isDefined) {
      mapToPhoenixTypeInstance(f.get.dt).toObject(src)
    } else {
      throw new UnsupportedOperationException(
        "Phoenix coder: without field metadata, 'fromBytes' conversion can not be supported")
    }
  }

  def toBytes(input: Any): Array[Byte] = {
    input match {
      case data: Boolean => PBoolean.INSTANCE.toBytes(data)
      case data: Byte => PTinyint.INSTANCE.toBytes(data)
      case data: Array[Byte] => PVarbinary.INSTANCE.toBytes(data)
      case data: Double => PDouble.INSTANCE.toBytes(data)
      case data: Float => PFloat.INSTANCE.toBytes(data)
      case data: Int => PInteger.INSTANCE.toBytes(data)
      case data: Long => PLong.INSTANCE.toBytes(data)
      case data: Short => PSmallint.INSTANCE.toBytes(data)
      case data: String => PVarchar.INSTANCE.toBytes(data)
      case _ => throw new UnsupportedOperationException(s"unsupported data type $input")
    }
  }

  override def isRowKeySupported(): Boolean = true

  override def isCompositeKeySupported(): Boolean = true

  override def decodeCompositeRowKey(row: Array[Byte], keyFields: Seq[Field]): Map[Field, Any] = {
    if (schema == null) schema = buildSchema(keyFields)
    val ptr: ImmutableBytesWritable = new ImmutableBytesWritable
    val maxOffest = schema.iterator(row, 0, row.length, ptr)
    var ret = Map.empty[Field, Any]
    for (i <- 0 until schema.getFieldCount) {
      if (schema.next(ptr, i, maxOffest) != null) {
        val value = mapToPhoenixTypeInstance(keyFields(i).dt)
          .toObject(ptr, schema.getField(i).getDataType, SortOrder.getDefault)
        ret += ((keyFields(i), value))
      }
    }
    ret
  }

  override def encodeCompositeRowKey(rkIdxedFields: Seq[(Int, Field)], row: Row): Seq[Array[Byte]] = {
    rkIdxedFields.map { case (x, y) =>
      var ret = toBytes(row(x))
      // the last dimension of composite key does not need SEPARATOR
      if (y.length == -1 && x < rkIdxedFields.size - 1)
        ret ++= QueryConstants.SEPARATOR_BYTE_ARRAY
      ret
    }
  }

  private def buildSchema(keyFields: Seq[Field]): RowKeySchema = {
    val builder: RowKeySchemaBuilder = new RowKeySchemaBuilder(keyFields.length)
    keyFields.foreach{ x =>
      builder.addField(buildPDatum(x.dt), false, SortOrder.getDefault)
    }
    builder.build
  }

  private def mapToPhoenixTypeInstance(input: DataType): PDataType[_] = {
    input match {
      case BooleanType => PBoolean.INSTANCE
      case ByteType => PTinyint.INSTANCE
      case DoubleType => PDouble.INSTANCE
      case IntegerType => PInteger.INSTANCE
      case FloatType => PFloat.INSTANCE
      case LongType => PLong.INSTANCE
      case ShortType => PSmallint.INSTANCE
      case StringType => PVarchar.INSTANCE
      case BinaryType => PVarbinary.INSTANCE
      case _ => throw new UnsupportedOperationException(s"unsupported data type $input")
    }
  }

  private def buildPDatum(input: DataType): PDatum = new PDatum {
    override def getScale: Integer = null
    override def isNullable: Boolean = false
    override def getDataType: PDataType[_] = mapToPhoenixTypeInstance(input)
    override def getMaxLength: Integer = null
    override def getSortOrder: SortOrder = SortOrder.getDefault
  }
}
