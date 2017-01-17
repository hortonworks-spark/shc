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

trait SHCDataType {
  // Parse the hbase Field to it's corresponding Scala type which can then be put into
  // a Spark GenericRow which is then automatically converted by Spark.
  def fromBytes(src: HBaseType): Any

  // Convert input to Byte Array (HBaseType)
  def toBytes(input: Any): Array[Byte]

  // If your data type do not need to support composite keys, you can just leave it empty or
  // threw an exception to remind users composite key is not supported.
  def isCompositeKeySupported(): Boolean

  def decodeCompositeRowKey(src: HBaseType, offset: Int, length: Int): Any

  def encodeCompositeRowKey(rkIdxedFields:Seq[(Int, Field)], row: Row): Seq[Array[Byte]]
}

/**
 * Currently, SHC supports three data types which can be used as serdes: Avro, Phoenix, PrimitiveType.
 * Adding New SHC data type should needs to implement the trait 'SHCDataType'.
 */
object SHCDataTypeFactory {

  def create(f: Field): SHCDataType = {
    if (f.fCoder == classOf[Avro].getSimpleName) {
      new Avro(Some(f))
    } else if (f.fCoder == classOf[Phoenix].getSimpleName) {
      new Phoenix(Some(f))
    } else if (f.fCoder == classOf[PrimitiveType].getSimpleName) {
      new PrimitiveType(Some(f))
    } else {
      // Data type implemented by user
      Class.forName(f.fCoder)
        .getConstructor(classOf[Option[Field]])
        .newInstance(f.fCoder)
        .asInstanceOf[SHCDataType]
    }
  }

  def create(coder: String): SHCDataType = {
    if (coder == classOf[Avro].getSimpleName) {
      new Avro()
    } else if (coder == classOf[Phoenix].getSimpleName) {
      new Phoenix()
    } else if (coder == classOf[PrimitiveType].getSimpleName) {
      new PrimitiveType()
    } else {
      // Data type implemented by user
      Class.forName(coder)
        .getConstructor(classOf[Option[Field]])
        .newInstance(None)
        .asInstanceOf[SHCDataType]
    }
  }
}
