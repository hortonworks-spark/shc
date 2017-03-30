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

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.util.{ThreadUtils, Utils}

final class HBaseCredentialsManager private() extends Logging {
  private class TokenInfo(
      val expireTime: Long,
      val conf: HBaseConfiguration,
      val token: Token[_ <: TokenIdentifier])
  private val tokensMap = new mutable.HashMap[String, TokenInfo]

  // We assume token expiration time should be no less than 10 minutes.
  private val nextRefresh = TimeUnit.MINUTES.toMillis(10)

  private val tokenUpdater =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("HBase Tokens Refresh Thread"))

  private val tokenUpdateRunnable = new Runnable {
    override def run(): Unit = Utils.logUncaughtExceptions(updateTokensIfRequired())
  }

  tokenUpdater.scheduleAtFixedRate(
    tokenUpdateRunnable, nextRefresh, nextRefresh, TimeUnit.MILLISECONDS)

  /**
   * Get HBase credential from specified cluster name.
   */
  def getCredentialsForCluster(
      hbaseCluster: String,
      hbaseConf: HBaseConfiguration): Credentials = synchronized {
    val credentials = new Credentials()

    val tokenOpt = this.synchronized {
      tokensMap.get(hbaseCluster)
    }

    // If token is existed and not expired, directly return the Credentials with tokens added in.
    if (tokenOpt.isDefined && !isTokenExpired(tokenOpt.get.expireTime)) {
      credentials.addToken(tokenOpt.get.token.getService, tokenOpt.get.token)
    } else {
      // Acquire a new token if not existed or old one is expired.
      val tokenInfo = getNewToken(hbaseConf)
      this.synchronized {
        tokensMap.put(hbaseCluster, tokenInfo)
      }
      logInfo(s"Obtain new token for cluster $hbaseCluster")

      credentials.addToken(tokenInfo.token.getService, tokenInfo.token)
    }

    credentials
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
    this.synchronized {
      try {
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
            (cluster, getNewToken(tokenInfo.conf))
          }

          this.synchronized {
            updatedTokens.foreach { kv => tokensMap.put(kv._1, kv._2) }
          }
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Error while trying to fetch tokens from HBase cluster", e)
      }
    }
  }

  private def getNewToken(hbaseConf: HBaseConfiguration): TokenInfo = {
    val token = TokenUtil.obtainToken(hbaseConf)
    val tokenIdentifier = token.decodeIdentifier()
    val expireTime =
      expectedExpireTime(tokenIdentifier.getIssueDate, tokenIdentifier.getExpirationDate)
    logInfo(s"Obtain new token with expiration time $expireTime")
    new TokenInfo(expireTime, hbaseConf, token)
  }
}

object HBaseCredentialsManager {
  lazy val manager = new  HBaseCredentialsManager
}


