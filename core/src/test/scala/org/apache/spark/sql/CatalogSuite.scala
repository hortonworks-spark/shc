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

package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

class CatalogSuite  extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging{
  def catalog = s"""{
            |"table":{"namespace":"default", "name":"table1", "tableCoder":"PrimitiveType"},
            |"rowkey":"key1:key2",
            |"columns":{
              |"col00":{"cf":"rowkey", "col":"key1", "type":"string", "length":"6"},
              |"col01":{"cf":"rowkey", "col":"key2", "type":"int"},
              |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
              |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
              |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
              |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
              |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
              |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"},
              |"col7":{"cf":"cf7", "col":"col7", "type":"string"}
            |}
          |}""".stripMargin

  test("Catalog meta data check") {
    val m = HBaseTableCatalog(Map(HBaseTableCatalog.tableCatalog->catalog))
    assert(m.row.fields.filter(_.length == -1).isEmpty)
    assert(m.row.length == 10)
  }

  test("Catalog should preserve the columns order") {
    val m = HBaseTableCatalog(Map(HBaseTableCatalog.tableCatalog->catalog))
    assert(m.toDataType.fields.map(_.name).sameElements(
      Array("col00", "col01", "col1", "col2", "col3", "col4", "col5", "col6", "col8", "col7")))
  }
}
