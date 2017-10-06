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

package org.apache.spark.sql.execution.datasources

import scala.math.Ordering

import org.apache.hadoop.hbase.util.Bytes

package object hbase {
  type HBaseType = Array[Byte]
  //Do not use BinaryType.ordering
  implicit val order: Ordering[HBaseType] =  ord


  val ord: Ordering[HBaseType] = new Ordering[HBaseType] {

    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      return Bytes.compareTo(x, y)
    }
  }

  val ByteMax = -1.asInstanceOf[Byte]
  val ByteMin = 0.asInstanceOf[Byte]
  val MaxLength = 256
}
