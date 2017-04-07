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

import java.io.{DataInputStream, ByteArrayInputStream}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.Date

import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.token.{AuthenticationTokenIdentifier, TokenUtil}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.hbase.SHCCredentialsManager._

final class SHCCredentialsManager private(sparkConf: SparkConf) extends Logging {
  private class TokenInfo(
    val expireTime: Long,
    val issueTime: Long,
    val refreshTime: Long,
    val conf: Configuration,
    val token: Token[_ <: TokenIdentifier],
    val serializedToken: Array[Byte])

  private val expireTimeFraction =sparkConf.getDouble(SparkHBaseConf.expireTimeFraction, 0.95)
  private val refreshTimeFraction = sparkConf.getDouble(SparkHBaseConf.refreshTimeFraction, 0.6)
  private val refreshDurationMins = sparkConf.getInt(SparkHBaseConf.refreshDurationMins, 10)

  private def isTokenInfoExpired(tokenInfo: TokenInfo): Boolean = {
    System.currentTimeMillis() >=
      ((tokenInfo.expireTime - tokenInfo.issueTime) * expireTimeFraction + tokenInfo.issueTime).toLong
  }

  private def getRefreshTime(issueTime: Long, expireTime: Long): Long = {
    require(expireTime > issueTime,
      s"Token expire time $expireTime is smaller than issue time $issueTime")

    // the expected expire time would be 60% of real expire time, to avoid long running task
    // failure.
    ((expireTime - issueTime) * refreshTimeFraction + issueTime).toLong
  }

  private val tokensMap = new mutable.HashMap[String, TokenInfo]

  // We assume token expiration time should be no less than 10 minutes.
  private val nextRefresh = TimeUnit.MINUTES.toMillis(refreshDurationMins)

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
  def getTokenForCluster(conf: Configuration): Array[Byte] = {
    val credentials = new Credentials()
    val identifier = clusterIdentifier(conf)
    var token: Array[Byte] = null

    val tokenOpt = this.synchronized {
      tokensMap.get(identifier)
    }

    // If token is existed and not expired, directly return the Credentials with tokens added in.
    if (tokenOpt.isDefined && !isTokenInfoExpired(tokenOpt.get)) {
      credentials.addToken(tokenOpt.get.token.getService, tokenOpt.get.token)
      token = tokenOpt.get.serializedToken
      logDebug(s"Use existing token for on-demand cluster $identifier")
    } else {

      logInfo(s"getCredentialsForCluster: Obtaining new token for cluster $identifier")

      // Acquire a new token if not existed or old one is expired.
      val tokenInfo = getNewToken(conf)
      this.synchronized {
        tokensMap.put(identifier, tokenInfo)
      }

      logInfo(s"getCredentialsForCluster: Obtained new token with expiration time" +
        s" ${new Date(tokenInfo.expireTime)} and refresh time ${new Date(tokenInfo.refreshTime)} " +
        s"for cluster $identifier")

      credentials.addToken(tokenInfo.token.getService, tokenInfo.token)
      token = tokenInfo.serializedToken
    }

    // the code will be rewritten or uncommented in the following PR
    // UserGroupInformation.getCurrentUser.addCredentials(credentials)

    token
  }

  def isCredentialsRequired(conf: Configuration): Boolean = {
    sparkConf.getBoolean(SparkHBaseConf.credentialsManagerEnabled, true) &&
      UserGroupInformation.isSecurityEnabled &&
        conf.get("hbase.security.authentication") == "kerberos"
  }

  private def updateTokensIfRequired(): Unit = {
    val currTime = System.currentTimeMillis()

    // Filter out all the tokens should be re-issued.
    val tokensToUpdate = this.synchronized {
      tokensMap.filter { case (_, tokenInfo) => tokenInfo.refreshTime <= currTime }
    }

    if (tokensToUpdate.isEmpty) {
      logInfo("Refresh Thread: No tokens require update")
    } else {
      // Update all the expect to be expired tokens
      val updatedTokens = tokensToUpdate.map { case (cluster, tokenInfo) =>
        logInfo(s"Refresh Thread: Update token for cluster $cluster")

        val token = {
          try {
            val tok = getNewToken(tokenInfo.conf)
            logInfo(s"Refresh Thread: Successfully obtained token for cluster $cluster")
            tok
          } catch {
            case NonFatal(ex) =>
              logWarning(s"Refresh Thread: Unable to fetch tokens from HBase cluster $cluster", ex)
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
    val token = TokenUtil.obtainToken(conf)
    val tokenIdentifier = token.decodeIdentifier()
    val expireTime = tokenIdentifier.getExpirationDate
    val issueTime = tokenIdentifier.getIssueDate
    val refreshTime = getRefreshTime(issueTime, expireTime)
    new TokenInfo(expireTime, issueTime, refreshTime, conf, token, serializeToken(token))
  }

  private def clusterIdentifier(conf: Configuration): String = {
    require(conf.get("zookeeper.znode.parent") != null &&
      conf.get("hbase.zookeeper.quorum") != null &&
      conf.get("hbase.zookeeper.property.clientPort") != null)

    conf.get("zookeeper.znode.parent") + "#"
      conf.get("hbase.zookeeper.quorum") + "#"
      conf.get("hbase.zookeeper.property.clientPort")
  }
}

object SHCCredentialsManager extends Logging {

  def get(sparkConf: SparkConf): SHCCredentialsManager = new SHCCredentialsManager(sparkConf)

  def serializeToken(token: Token[_ <: TokenIdentifier]): Array[Byte] = {
    val dob: DataOutputBuffer = new DataOutputBuffer()
    token.write(dob)
    val dobCopy = new Array[Byte](dob.getLength)
    System.arraycopy(dob.getData, 0, dobCopy, 0, dobCopy.length)
    dobCopy
  }

  def deserializeToken(tokenBytes: Array[Byte]): Token[_ <: TokenIdentifier] = {
    val byteStream = new ByteArrayInputStream(tokenBytes)
    val dataStream = new DataInputStream(byteStream)
    val destToken = new Token
    destToken.readFields(dataStream)
    destToken
  }
}
