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

package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, Logging}

class InsertDataSuite extends SHC with Logging {

  def withCatalog(cat: String): DataFrame = {
    sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def persistDataInHBase(cat: String, data: Seq[HBaseRecord]): Unit = {
    val sql = sqlContext
    import sql.implicits._
    sc.parallelize(data).toDF.write
      .options(Map(
        HBaseTableCatalog.newTable -> "5",
        HBaseTableCatalog.tableCatalog -> cat
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("inserting data with null values") {
    val withNullData = (1 to 2).map(HBaseRecord(_, "").copy(col7 = null))
    val withoutNullData = (3 to 4).map(HBaseRecord(_, "not null"))

    persistDataInHBase(catalog, withNullData)
    persistDataInHBase(catalog, withoutNullData)

    val data: DataFrame = withCatalog(catalog)

    assert(data.count() == 4)

    val rows = data.take(10)
    assert(rows.count(_.getString(7) == null) == 2)
    assert(rows.count(_.getString(7) != null) == 2)
  }
}
