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

package org.apache.spark.sql.execution.datasources.hbase

import scala.collection.JavaConverters._
import scala.reflect.runtime._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{TokenIdentifier, Token}
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class SHCCredentialProvider extends Logging {
  def serviceName: String = "shc"

  def credentialsRequired(conf: Configuration): Boolean = {
    if (conf != null) {
      hbaseConf(conf).get("hbase.security.authentication") == "kerberos"
    } else {
      false
    }
  }

  def obtainCredentials(
      conf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      val obtainToken = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
        getMethod("obtainToken", classOf[Configuration])

      logDebug("Attempting to fetch HBase security token.")
      val token = obtainToken.invoke(null, hbaseConf(conf))
        .asInstanceOf[Token[_ <: TokenIdentifier]]
      logInfo(s"Get token from HBase: ${token.toString}")
      creds.addToken(token.getService, token)
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to get token from service $serviceName", e)
    }

    val expirationDates = creds.getAllTokens.asScala
      .filter(_.decodeIdentifier().isInstanceOf[AuthenticationTokenIdentifier])
      .map { t =>
        val identifier = t.decodeIdentifier().asInstanceOf[AuthenticationTokenIdentifier]
        identifier.getExpirationDate
      }
    if (expirationDates.isEmpty)
      None
    else
      Some(expirationDates.min)
  }

  def getOrCreate(hbaseConf: Configuration): Credentials = {
    HBaseConnectionCache.getConnection(hbaseConf).credentials
  }

  private def hbaseConf(conf: Configuration): Configuration = {
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      val confCreate = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
        getMethod("create", classOf[Configuration])
      confCreate.invoke(null, conf).asInstanceOf[Configuration]
    } catch {
      case NonFatal(e) =>
        logDebug("Fail to invoke HBaseConfiguration", e)
        conf
    }
  }
}
