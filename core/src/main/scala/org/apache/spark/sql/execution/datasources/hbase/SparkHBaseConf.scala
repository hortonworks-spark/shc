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

package org.apache.spark.sql.execution.datasources.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.datasources.hbase.types._


object SparkHBaseConf {
  val testConf = "spark.hbase.connector.test"
  val credentialsManagerEnabled = "spark.hbase.connector.security.credentials.enabled"
  val expireTimeFraction = "spark.hbase.connector.security.credentials.expireTimeFraction"
  val refreshTimeFraction = "spark.hbase.connector.security.credentials.refreshTimeFraction"
  val refreshDurationMins = "spark.hbase.connector.security.credentials.refreshDurationMins"
  val principal = "spark.hbase.connector.security.credentials"
  val keytab = "spark.hbase.connector.security.keytab"

  var conf: Configuration = _
  var BulkGetSize = "spark.hbase.connector.bulkGetSize"
  var defaultBulkGetSize = 100
  var CachingSize = "spark.hbase.connector.cacheSize"
  var defaultCachingSize = 100
  // in milliseconds
  val connectionCloseDelay = 10 * 60 * 1000

  // for SHC DataType
  val Avro = classOf[Avro].getSimpleName
  val Phoenix = classOf[Phoenix].getSimpleName
  val PrimitiveType = classOf[PrimitiveType].getSimpleName
}
