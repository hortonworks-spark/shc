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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.token.{AuthenticationTokenIdentifier, TokenUtil}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.util.{ThreadUtils, Utils}

final class SHCCredentialsManager private() extends Logging {
  private class TokenInfo(
    val expireTime: Long,
    val issueTime: Long,
    val conf: Configuration,
    val token: Token[_ <: TokenIdentifier]) {

    def isTokenInfoExpired: Boolean = {
      System.currentTimeMillis() >=
        ((expireTime - issueTime) * SHCCredentialsManager.expireTimeFraction + issueTime).toLong
    }

    val refreshTime: Long = {
      require(expireTime > issueTime,
        s"Token expire time $expireTime is smaller than issue time $issueTime")

      // the expected expire time would be 60% of real expire time, to avoid long running task
      // failure.
      ((expireTime - issueTime) * SHCCredentialsManager.refreshTimeFraction + issueTime).toLong
    }
  }

  private val tokensMap = new mutable.HashMap[String, TokenInfo]

  // We assume token expiration time should be no less than 10 minutes.
  private val nextRefresh = TimeUnit.MINUTES.toMillis(SHCCredentialsManager.refreshDurationMins)

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
  def getCredentialsForCluster(conf: Configuration): Array[Byte] = {
    val credentials = new Credentials()
    val identifier = clusterIdentifier(conf)

    val tokenOpt = this.synchronized {
      tokensMap.get(identifier)
    }

    // If token is existed and not expired, directly return the Credentials with tokens added in.
    if (tokenOpt.isDefined && !tokenOpt.get.isTokenInfoExpired) {
      credentials.addToken(tokenOpt.get.token.getService, tokenOpt.get.token)
      logInfo(s"Use existing token for on-demand cluster $identifier")
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
    }

    UserGroupInformation.getCurrentUser.addCredentials(credentials)
    SHCCredentialsManager.addLogs("Driver", credentials) // Only driver invokes getCredentialsForCluster
    SHCCredentialsManager.serialize(credentials)
  }

  def isCredentialsRequired(conf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled &&
      conf.get("hbase.security.authentication") == "kerberos"
  }

  private def updateTokensIfRequired(): Unit = {
    val currTime = System.currentTimeMillis()

    // Filter out all the tokens should be re-issued.
    val tokensShouldUpdate = this.synchronized {
      tokensMap.filter { case (_, tokenInfo) => tokenInfo.refreshTime <= currTime }
    }

    if (tokensShouldUpdate.isEmpty) {
      logInfo("Refresh Thread: No tokens require update")
    } else {
      // Update all the expect to be expired tokens
      val updatedTokens = tokensShouldUpdate.map { case (cluster, tokenInfo) =>
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
    new TokenInfo(expireTime, issueTime, conf, token)
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

object SHCCredentialsManager extends Logging {
  lazy val manager = new  SHCCredentialsManager

  private val expireTimeFraction = 0.95
  private val refreshTimeFraction = 0.6
  private val refreshDurationMins = 10

  def serialize(credentials: Credentials): Array[Byte] = {
    if (credentials != null) {
      val dob = new DataOutputBuffer()
      credentials.writeTokenStorageToStream(dob)
      val dobCopy = new Array[Byte](dob.getLength)
      System.arraycopy(dob.getData, 0, dobCopy, 0, dobCopy.length)
      dobCopy
    } else {
      null
    }
  }

  def deserialize(credsBytes: Array[Byte]): Credentials = {
    if (credsBytes != null) {
      val byteStream = new ByteArrayInputStream(credsBytes)
      val dataStream = new DataInputStream(byteStream)
      val credentials = new Credentials()
      credentials.readTokenStorageStream(dataStream)
      credentials
    } else {
      null
    }
  }

  // for debug
  def addLogs(component: String, credentials: Credentials): Unit = {
    logInfo(s"$component: Obtain credentials with minimum expiration date of " +
      s"tokens ${getMinimumExpirationDates(credentials).getOrElse(-1)}")
  }

  private def getMinimumExpirationDates (credentials: Credentials): Option[Long] = {
    val expirationDates = credentials.getAllTokens.asScala
      .filter(_.decodeIdentifier().isInstanceOf[AuthenticationTokenIdentifier])
      .map { t =>
        val identifier = t.decodeIdentifier().asInstanceOf[AuthenticationTokenIdentifier]
        identifier.getExpirationDate
      }
    if (expirationDates.isEmpty) None else Some(expirationDates.min)
  }
}
