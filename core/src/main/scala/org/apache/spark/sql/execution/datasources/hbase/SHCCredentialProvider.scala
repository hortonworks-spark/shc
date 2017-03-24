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

import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import scala.reflect.runtime._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{TokenIdentifier, Token}
import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider

class SHCCredentialProvider extends ServiceCredentialProvider with Logging {
  override def serviceName: String = "shc"

  private var tokenRenewalInterval: Option[Long] = null

  override def credentialsRequired(hadoopConf: Configuration): Boolean = {
    hbaseConf(hadoopConf).get("hbase.security.authentication") == "kerberos"
  }

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)
      val obtainToken = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
        getMethod("obtainToken", classOf[Configuration])

      logDebug("Attempting to fetch HBase security token.")
      val token = obtainToken.invoke(null, hbaseConf(hadoopConf))
        .asInstanceOf[Token[_ <: TokenIdentifier]]
      logInfo(s"Get token from HBase: ${token.toString}")
      creds.addToken(token.getService, token)
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to get token from service $serviceName", e)
    }

    // Get the token renewal interval if it is not set. It will only be called once.
    if (tokenRenewalInterval == null) {
      tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, sparkConf)
    }

    // Get the time of next renewal.
    val nextRenewalDate =
      /*tokenRenewalInterval.flatMap { interval =>
      val nextRenewalDates = creds.getAllTokens.asScala
        .filter(_.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier])
        .map { t =>
          val identifier = t.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          identifier.getIssueDate + interval
        }
      if (nextRenewalDates.isEmpty) None else Some(nextRenewalDates.min)
    }*/

    nextRenewalDate
    None
  }

  // In progress
  private def getTokenRenewalInterval(HbaseConf: Configuration, sparkConf: SparkConf): Option[Long] = {
    None
  }

  // In progress
  private def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    delegTokenRenewer
  }

  // In progress
  def getOrCreat(HbaseConf: Configuration): Credentials = {
    val key = new HBaseConnectionKey(HbaseConf)
    if(HBaseConnectionCache.connectionMap.contains(key)) {
      val connection = HBaseConnectionCache.connectionMap.get(key)
      val cret = HBaseConnectionCache.credentialsMap.get(connection.get).get
      if (cret == null) {
        val cretNew = new Credentials()
        obtainCredentials(Configuration, SparkEnv.get.conf, cretNew)
        HBaseConnectionCache.credentialsMap.put(connection.get, cretNew)
        cretNew
      } else {
        cret
      }
    } else {
      null
    }
  }

  private def hbaseConf(conf: Configuration): Configuration = {
    try {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)
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