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

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.SparkConf
import org.apache.spark.util.{ThreadUtils, Utils}

final class HBaseCredentialsManager private(sparkConf: SparkConf) extends Logging {
  private class TokenInfo(
      val expireTime: Long,
      val conf: Configuration,
      val token: Token[_ <: TokenIdentifier])
  private val tokensMap = new mutable.HashMap[String, TokenInfo]

  // We assume token expiration time should be no less than 10 minutes.
  private val nextRefresh = TimeUnit.MINUTES.toMillis(10)

  private val tokenUpdater =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("HBase Tokens Refresh Thread"))

  require(sparkConf.contains("spark.yarn.principal"))
  require(sparkConf.contains("spark.yarn.keytab"))
  private val principal = sparkConf.get("spark.yarn.principal")
  private val keytab = sparkConf.get("spark.yarn.keytab")

  private val tokenUpdateRunnable = new Runnable {
    override def run(): Unit = Utils.logUncaughtExceptions(updateTokensIfRequired())
  }

  tokenUpdater.scheduleAtFixedRate(
    tokenUpdateRunnable, nextRefresh, nextRefresh, TimeUnit.MILLISECONDS)

  /**
   * Get HBase credential from specified cluster name.
   */
  def getCredentialsForCluster(conf: Configuration): Credentials = {
    val credentials = new Credentials()
    val identifier = clusterIdentifier(conf)

    val tokenOpt = this.synchronized {
      tokensMap.get(identifier)
    }

    // If token is existed and not expired, directly return the Credentials with tokens added in.
    if (tokenOpt.isDefined && !isTokenExpired(tokenOpt.get.expireTime)) {
      credentials.addToken(tokenOpt.get.token.getService, tokenOpt.get.token)
    } else {
      // Acquire a new token if not existed or old one is expired.
      val tokenInfo = getNewToken(conf)
      this.synchronized {
        tokensMap.put(identifier, tokenInfo)
      }
      logInfo(s"Obtain new token for cluster $identifier")

      credentials.addToken(new Text(identifier), tokenInfo.token)
    }

    credentials
  }

  def isCredentialsRequired(conf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled &&
      conf.get("hbase.security.authentication") == "kerberos"
  }

  private def isTokenExpired(expireTime: Long): Boolean = {
    System.currentTimeMillis() >= expireTime
  }

  private def expectedExpireTime(issueTime: Long, expireTime: Long): Long = {
    require(expireTime > issueTime,
      s"Token expire time $expireTime is smaller than issue time $issueTime")

    // the expected expire time would be 60% of real expire time, to avoid long running task
    // failure.
    ((expireTime - issueTime) * 0.6 + issueTime).toLong
  }

  private def updateTokensIfRequired(): Unit = {
    val currTime = System.currentTimeMillis()

    // Filter out all the tokens should be re-issued.
    val tokensShouldUpdate = this.synchronized {
      tokensMap.filter { case (_, tokenInfo) => tokenInfo.expireTime <= currTime }
    }

    if (tokensShouldUpdate.isEmpty) {
      logDebug(s"No token requires update now $currTime")
    } else {
      // Update all the expect to be expired tokens
      val updatedTokens = tokensShouldUpdate.map { case (cluster, tokenInfo) =>
        logInfo(s"Update token for cluster $cluster")
        val token = {
          try {
            getNewToken(tokenInfo.conf)
          } catch {
            case NonFatal(ex) =>
              logWarning("Error while trying to fetch tokens from HBase cluster", ex)
              null
          }
        }
        (cluster, token)
      }.filter(null != _._2)

      this.synchronized {
        updatedTokens.foreach { kv => tokensMap.put(kv._1, kv._2) }
      }
    }
  }

  private def getNewToken(conf: Configuration): TokenInfo = {
    val kerberosLoggedUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
    kerberosLoggedUGI.doAs(new PrivilegedExceptionAction[TokenInfo] {
      override def run(): TokenInfo = {
        val token = TokenUtil.obtainToken(conf)
        val tokenIdentifier = token.decodeIdentifier()
        val expireTime =
          expectedExpireTime(tokenIdentifier.getIssueDate, tokenIdentifier.getExpirationDate)
        logInfo(s"Obtain new token with expiration time $expireTime")
        new TokenInfo(expireTime, conf, token)
      }
    })
  }

  private def clusterIdentifier(conf: Configuration): String = {
    require(conf.get("zookeeper.znode.parent") != null &&
      conf.get("hbase.zookeeper.quorum") != null &&
      conf.get("hbase.zookeeper.property.clientPort") != null)

    conf.get("zookeeper.znode.parent") + "-"
      conf.get("hbase.zookeeper.quorum") + "-"
      conf.get("hbase.zookeeper.property.clientPort")
  }
}

object HBaseCredentialsManager {
  @volatile private var _hbaseCredentialManager: HBaseCredentialsManager = _

  def get(sparkConf: SparkConf): HBaseCredentialsManager = {
    if (_hbaseCredentialManager == null) {
      HBaseCredentialsManager.synchronized {
        if (_hbaseCredentialManager == null) {
          _hbaseCredentialManager = new HBaseCredentialsManager(sparkConf)
        }
      }
    }
    _hbaseCredentialManager
  }
}
