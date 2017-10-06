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

package org.apache.spark.sql.execution.datasources.hbase.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.hbase._

trait SHCDataType extends Serializable {
  // Parse the hbase Field to it's corresponding Scala type which can then be put into
  // a Spark GenericRow which is then automatically converted by Spark.
  def fromBytes(src: HBaseType): Any

  // Convert input to Byte Array (HBaseType)
  def toBytes(input: Any): Array[Byte]

  // If lexicographic sort order is maintained, then return true.
  // If return false, the data type can not be the table coder.
  def isRowKeySupported(): Boolean = false

  def isCompositeKeySupported(): Boolean = false

  /**
   * Takes a HBase Row object and parses all of the fields from it.
   * This is independent of which fields were requested from the key
   * Because we have all the data it's less complex to parse everything.
   *
   * @param keyFields all of the fields in the row key, ORDERED by their order in the row key.
   */
  def decodeCompositeRowKey(row: Array[Byte], keyFields: Seq[Field]): Map[Field, Any] = {
    throw new UnsupportedOperationException("Composite key is not supported")
  }

  def encodeCompositeRowKey(rkIdxedFields:Seq[(Int, Field)], row: Row): Seq[Array[Byte]] = {
    throw new UnsupportedOperationException("Composite key is not supported")
  }
}

/**
 * Currently, SHC supports three data types which can be used as serdes: Avro, Phoenix, PrimitiveType.
 * Adding New SHC data type needs to implement the trait 'SHCDataType'.
 */
object SHCDataTypeFactory {

  def create(f: Field): SHCDataType = {
    if (f == null) {
      throw new NullPointerException(
        "SHCDataTypeFactory: the 'f' parameter used to create SHCDataType " +
          "can not be null.")
    }

    if (f.fCoder == SparkHBaseConf.Avro) {
      new Avro(Some(f))
    } else if (f.fCoder == SparkHBaseConf.Phoenix) {
      new Phoenix(Some(f))
    } else if (f.fCoder == SparkHBaseConf.PrimitiveType) {
      new PrimitiveType(Some(f))
    } else {
      // Data type implemented by user
      Class.forName(f.fCoder)
        .getConstructor(classOf[Option[Field]])
        .newInstance(Some(f))
        .asInstanceOf[SHCDataType]
    }
  }

  // Currently, the function below is only used for creating the table coder.
  // One catalog/HBase table can only use one table coder, so the function is
  // only called once in 'HBaseTableCatalog' class.
  def create(coder: String): SHCDataType = {
    if (coder == null || coder.isEmpty) {
      throw new NullPointerException(
        "SHCDataTypeFactory: the 'coder' parameter used to create SHCDataType " +
          "can not be null or empty.")
    }

    if (coder == SparkHBaseConf.Avro) {
      new Avro()
    } else if (coder == SparkHBaseConf.Phoenix) {
      new Phoenix()
    } else if (coder == SparkHBaseConf.PrimitiveType) {
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
