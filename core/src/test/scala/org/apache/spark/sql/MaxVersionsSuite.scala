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
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
import org.apache.spark.sql.sources.PrunedFilteredScan


class MaxVersionsSuite extends SHC with Logging {

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  private def prunedFilterScan(cat: String): PrunedFilteredScan = {
    HBaseRelation(Map(HBaseTableCatalog.tableCatalog->cat),None)(sqlContext)
  }

  test("Max Versions semantics") {
    val sql = sqlContext
    import sql.implicits._

    val oldestMs = 754869600000L
    val oldMs = 754869611111L
    val newMs = 754869622222L
    val newestMs = 754869633333L

    val oldestData = (0 to 2).map { i =>
      HBaseRecord(i, "ancient")
    }
    val oldData = (0 to 2).map { i =>
      HBaseRecord(i, "old")
    }
    val newData = (0 to 2).map { i =>
      HBaseRecord(i, "new")
    }
    val newestData = (0 to 1).map { i =>
      HBaseRecord(i, "latest")
    }

    sc.parallelize(oldestData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5", HBaseRelation.MAX_VERSIONS -> "3", HBaseRelation.TIMESTAMP -> oldestMs.toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sc.parallelize(oldData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5", HBaseRelation.MAX_VERSIONS -> "3", HBaseRelation.TIMESTAMP -> oldMs.toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sc.parallelize(newData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5", HBaseRelation.MAX_VERSIONS -> "3", HBaseRelation.TIMESTAMP -> newMs.toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sc.parallelize(newestData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5", HBaseRelation.MAX_VERSIONS -> "3", HBaseRelation.TIMESTAMP -> newestMs.toString))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    // Test specific last two versions
    val twoVersions: DataFrame = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog, HBaseRelation.MAX_VERSIONS -> "2", HBaseRelation.MARGE_TO_LATEST -> "false"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //count is made on HBase directly and return number of unique rows
    assert(twoVersions.count() == 3)

    val rows = twoVersions.take(10)
    assert(rows.size == 6)
    assert(rows.filter(row => row.getString(7).contains("ancient")).size == 0)
    assert(rows.filter(row => row.getString(7).contains("old")).size == 1)
    assert(rows.filter(row => row.getString(7).contains("new")).size == 3)
    assert(rows.filter(row => row.getString(7).contains("latest")).size == 2)

    //we cannot take more then three because we create table with that size
    val threeVersions: DataFrame = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog, HBaseRelation.MAX_VERSIONS -> "4", HBaseRelation.MARGE_TO_LATEST -> "false"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //count is made on HBase directly and return number of unique rows
    assert(threeVersions.count() == 3)

    val threeRows = threeVersions.take(10)
    assert(threeRows.size == 9)
    assert(threeRows.filter(row => row.getString(7).contains("ancient")).size == 1)
    assert(threeRows.filter(row => row.getString(7).contains("old")).size == 3)
    assert(threeRows.filter(row => row.getString(7).contains("new")).size == 3)
    assert(threeRows.filter(row => row.getString(7).contains("latest")).size == 2)

    // Test specific only last versions
    val lastVersions: DataFrame = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val lastRows = lastVersions.take(10)
    assert(lastRows.size == 3)
    assert(lastRows.filter(row => row.getString(7).contains("new")).size == 1)
    assert(lastRows.filter(row => row.getString(7).contains("latest")).size == 2)


  }

}
