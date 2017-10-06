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

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

class DataTypeConverter extends SHC with Logging{
  ignore("Basic setup") {
    val spark = SparkSession.builder()
      .master("local")
      .appName("HBaseTest")
      .config(conf)
      .getOrCreate()
    val sqlContext = spark.sqlContext

    val complex = s"""MAP<int, struct<varchar:string>>"""
    val schema =
      s"""{"namespace": "example.avro",
         |   "type": "record", "name": "User",
         |    "fields": [ {"name": "name", "type": "string"},
         |      {"name": "favorite_number",  "type": ["int", "null"]},
         |        {"name": "favorite_color", "type": ["string", "null"]} ]}""".stripMargin

    val catalog = s"""{
            |"table":{"namespace":"default", "name":"htable", "tableCoder":"PrimitiveType"},
            |"rowkey":"key1:key2",
            |"columns":{
              |"col1":{"cf":"rowkey", "col":"key1", "type":"binary"},
              |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
              |"col3":{"cf":"cf1", "col":"col1", "avro":"schema1"},
              |"col4":{"cf":"cf1", "col":"col2", "type":"string"},
              |"col5":{"cf":"cf1", "col":"col3", "type":"double"},
              |"col6":{"cf":"cf1", "col":"col4", "type":"$complex"}
            |}
          |}""".stripMargin
    val df =
      sqlContext.read.options(
        Map("schema1"->schema, HBaseTableCatalog.tableCatalog->catalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    df.write.options(
      Map("schema1"->schema, HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //val s = df.filter((($"col1" < Array(10.toByte)) and ($"col1" > Array(1.toByte))) or ($"col1" === Array(11.toByte))).select("col1")
    //val s = df.filter(Column("col1").<(Array(10.toByte)).and(Column("col1").>(Array(1.toByte))).or(Column("col1") === Array(11.toByte))).select("col1")
    // val s = df.filter((($"col1" < Array(10.toByte)) && ($"col1" > Array(1.toByte))) || ($"col1" === Array(11.toByte))).select("col1")
    //val s = df.filter(($"col1" < Array(10.toByte) && $"col1" > Array(1.toByte)) || $"col1" === Array(11.toByte) || $"col2" === 2.3).select("col1") // range should be (None, None)
    val s = df.filter(($"col1" < Array(10.toByte) &&
      $"col1" > Array(1.toByte)) ||
      $"col1" === Array(11.toByte) &&
        $"col2" === 2.3)
      .select("col1")
    s.count()
    df.createOrReplaceTempView("table")
    val c = sqlContext.sql("select count(col1) from table")
    // c.queryExecution
    c.show
    val se = df.filter($"col2" > 12).filter($"col4" < Array(10.toByte)).select("col1")

    val se1 = df.filter($"col2" > 12).filter($"col4" < Array(10.toByte)).select("col1")
    se.count()
    se1.collect.foreach(println(_))
    println(df)
  }
}
