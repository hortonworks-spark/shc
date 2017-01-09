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

import org.apache.phoenix.schema.types.{PFloat, PInteger}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.types.{FloatType, IntegerType}

class Phoenix(f: Field) extends SHCDataType {

  def fromBytes(src: HBaseType): Any = {
    f.dt match {
      case IntegerType => PInteger.INSTANCE.toObject(src)
      case FloatType => PFloat.INSTANCE.toObject(src)
    }
  }

  def toBytes(input: Any): Array[Byte] = {
    input match {
      case IntegerType => PInteger.INSTANCE.toBytes(input)
      case FloatType => PFloat.INSTANCE.toBytes(input)
    }
  }

  def fromCompositeKeyToObject(src: HBaseType, offset: Int, length: Int): Any = {
    throw new Exception("Not Support yet")
  }
}
