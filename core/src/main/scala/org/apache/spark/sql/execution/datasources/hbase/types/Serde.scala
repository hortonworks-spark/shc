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

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase.util.Bytes

/*
 * Currently, as an example, SHC only implemented the 'DoubleSerde' for 'Double' data type.
 * TODO: more serdes for other data types.
 */
abstract class Serde(f: Field,
            offset: Option[Int] = Some(0),
            length: Option[Int] = None) extends SHCDataType {
  // serialize
  def toObject(src: HBaseType): Any
  // deserialize
  def toBytes(input: Any): Array[Byte]
}

case class DoubleSerde(f: Field,
                        start: Option[Int] = Some(0),
                        length: Option[Int] = None) extends Serde(f, start, length) {

  override def toBytes(value: Any): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Double])
  override def toObject(src: HBaseType): Any = Bytes.toLong(src, start.get)
}
