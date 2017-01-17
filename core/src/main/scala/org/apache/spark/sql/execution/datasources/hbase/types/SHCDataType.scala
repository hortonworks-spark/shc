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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.hbase._

import scala.util.control.NonFatal

trait SHCDataType {
  // Parse the hbase Field to it's corresponding Scala type which can then be put into
  // a Spark GenericRow which is then automatically converted by Spark.
  def bytesToColumn(src: HBaseType): Any

  // Convert input to Byte Array (HBaseType)
  def toBytes(input: Any): Array[Byte]

  // If your data type do not need to support composite keys, you can just leave it empty or
  // threw an exception to remind users composite key is not supported.
  def isCompositeKeySupported(): Boolean
  def bytesToCompositeKeyField(src: HBaseType, offset: Int, length: Int): Any
  def encodeCompositeRowKey(rkIdxedFields:Seq[(Int, Field)], row: Row): Seq[Array[Byte]]
}

/**
 * Currently, SHC supports three serdes: Avro, Phoenix, PrimitiveType.
 * Adding New SHC serde should need to implement the trait 'SHCDataType'.
 */
object SHCDataTypeFactory {
  def create(f: Field): SHCDataType = {
    if (f.fCoder == "Avro") {
      new Avro(Some(f))
    } else if (f.fCoder == "Phoenix") {
      new Phoenix(Some(f))
    } else if (f.fCoder == "PrimitiveType") {
      new PrimitiveType(Some(f))
    } else {
      Class.forName(s"org.apache.spark.sql.execution.datasources.hbase.types.${f.fCoder}")
        .getConstructor(classOf[Option[Field]])
        .newInstance(f.fCoder)
        .asInstanceOf[SHCDataType]
    }
  }

  def create(coder: String): SHCDataType = {
    if (coder == "Avro") {
      new Avro()
    } else if (coder == "Phoenix") {
      new Phoenix()
    } else if (coder == "PrimitiveType") {
      new PrimitiveType()
    } else {
      Class.forName(s"org.apache.spark.sql.execution.datasources.hbase.types.$coder")
        .getConstructor(classOf[Option[Field]])
        .newInstance(None)
        .asInstanceOf[SHCDataType]
    }
  }
}
