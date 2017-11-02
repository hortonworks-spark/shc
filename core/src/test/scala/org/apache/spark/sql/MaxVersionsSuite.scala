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

import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}

class MaxVersionsSuite extends SHC with Logging {

  def withCatalog(cat: String, options: Map[String,String]): DataFrame = {
    sqlContext.read
      .options(options ++ Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def persistDataInHBase(cat: String, data: Seq[HBaseRecord], timestamp: Long): Unit = {
    val sql = sqlContext
    import sql.implicits._
    sc.parallelize(data).toDF.write
      .options(Map(
        HBaseTableCatalog.tableCatalog -> cat,
        HBaseTableCatalog.newTable -> "5",
        HBaseRelation.MAX_VERSIONS -> "3",
        HBaseRelation.TIMESTAMP -> timestamp.toString
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("Max Versions semantics") {

    val oldestMs = 754869600000L
    val oldMs = 754869611111L
    val newMs = 754869622222L
    val newestMs = 754869633333L

    val oldestData = (0 to 2).map(HBaseRecord(_, "ancient"))
    val oldData = (0 to 2).map(HBaseRecord(_, "old"))
    val newData = (0 to 2).map(HBaseRecord(_, "new"))
    val newestData = (0 to 1).map(HBaseRecord(_, "latest"))

    persistDataInHBase(catalog, oldestData, oldestMs)
    persistDataInHBase(catalog, oldData, oldMs)
    persistDataInHBase(catalog, newData, newMs)
    persistDataInHBase(catalog, newestData, newestMs)

    // Test specific last two versions
    val twoVersions: DataFrame = withCatalog(catalog, Map(
      HBaseRelation.MAX_VERSIONS -> "2",
      HBaseRelation.MERGE_TO_LATEST -> "false"
    ))

    //count is made on HBase directly and return number of unique rows
    assert(twoVersions.count() == 3)

    val rows = twoVersions.take(10)
    assert(rows.size == 6)
    assert(rows.count(_.getString(7).contains("ancient")) == 0)
    assert(rows.count(_.getString(7).contains("old")) == 1)
    assert(rows.count(_.getString(7).contains("new")) == 3)
    assert(rows.count(_.getString(7).contains("latest")) == 2)

    //we cannot take more then three because we create table with that size
    val threeVersions: DataFrame = withCatalog(catalog, Map(
      HBaseRelation.MAX_VERSIONS -> "4",
      HBaseRelation.MERGE_TO_LATEST -> "false"
    ))

    val threeRows = threeVersions.take(10)
    assert(threeRows.size == 9)
    assert(threeRows.count(_.getString(7).contains("ancient")) == 1)
    assert(threeRows.count(_.getString(7).contains("old")) == 3)
    assert(threeRows.count(_.getString(7).contains("new")) == 3)
    assert(threeRows.count(_.getString(7).contains("latest")) == 2)

    // Test specific only last versions
    val lastVersions: DataFrame = withCatalog(catalog, Map.empty)

    val lastRows = lastVersions.take(10)
    assert(lastRows.size == 3)
    assert(lastRows.count(_.getString(7).contains("new")) == 1)
    assert(lastRows.count(_.getString(7).contains("latest")) == 2)
  }
}
