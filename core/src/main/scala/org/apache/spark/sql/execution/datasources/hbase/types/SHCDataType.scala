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

trait SHCDataType {
  // Parses the hbase field to it's corresponding scala type which can then be put into
  // a Spark GenericRow which is then automatically converted by Spark.
  def toObject: Any

  // Convert input to data type
  def toBytes(input: Any): Array[Byte]
}

/**
  * SHC supports different data formats like Avro, etc. Different data formats use different SHC data types
  * to do data type conversions. 'Atomic' is for normal scala data types (Int, Long, etc). 'Avro' is for
  * Avro data format. 'Phoenix' is for Phoenix data types (https://phoenix.apache.org/language/datatypes.html).
  * 'Serde' is used to do data type conversions specified by users, and users should extend the abstract class 'Serde'.
  * New SHC data types should implement the trait SHCDataType.
  */
object SHDDataTypeFactory {
  def create(f: Field,
             src: Option[HBaseType] = None,
             offset: Option[Int] = None,
             length: Option[Int] = None): SHCDataType = {

    // If we already have sedes defined, use it.
    if (f.serde.isDefined)
      Class.forName(f.serde.get)
        .getDeclaredConstructor(Field.getClass,
          classOf[Option[HBaseType]],
          classOf[Option[Int]],
          classOf[Option[Int]])
        .newInstance(f, src, offset, length)
        .asInstanceOf[SHCDataType]
    else if (f.exeSchema.isDefined)
      new Avro(f, src)
    else if (f.phoenix.isDefined)
      new Phoenix(f, src)
    else
      new AtomicType(f, src, offset, length)
  }
}