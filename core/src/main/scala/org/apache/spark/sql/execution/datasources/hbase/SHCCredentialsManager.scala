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

package org.apache.spark.sql.execution.datasources.hbase

import java.io.{ByteArrayInputStream, DataInputStream}
import java.security.PrivilegedExceptionAction
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
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.datasources.hbase.SHCCredentialsManager._

final class SHCCredentialsManager private() extends Logging {
  private class TokenInfo(
    val expireTime: Long,
    val issueTime: Long,
    val refreshTime: Long,
    val conf: Configuration,
    val token: Token[_ <: TokenIdentifier],
    val serializedToken: Array[Byte])

  private def sparkConf = SparkEnv.get.conf
  private val expireTimeFraction = sparkConf.getDouble(SparkHBaseConf.expireTimeFraction, 0.95)
  private val refreshTimeFraction = sparkConf.getDouble(SparkHBaseConf.refreshTimeFraction, 0.6)
  private val refreshDurationMins = sparkConf.getInt(SparkHBaseConf.refreshDurationMins, 10)

  private val tokensMap = new mutable.HashMap[String, TokenInfo]

  // We assume token expiration time should be no less than 10 minutes by default.
  private val nextRefresh = TimeUnit.MINUTES.toMillis(refreshDurationMins)

  private val credentialsManagerEnabled = {
    val isEnabled = sparkConf.getBoolean(SparkHBaseConf.credentialsManagerEnabled, false) &&
      UserGroupInformation.isSecurityEnabled
    logInfo(s"SHCCredentialsManager was${if (isEnabled) "" else " not"} enabled.")
    isEnabled
  }

  val tokenUpdateExecutor = Executors.newSingleThreadScheduledExecutor(
    ThreadUtils.namedThreadFactory("HBase Tokens Refresh Thread"))

  // If SHCCredentialsManager is enabled, start an executor to update tokens
  if (credentialsManagerEnabled) {
    val tokenUpdateRunnable = new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions(updateTokensIfRequired())
    }
    tokenUpdateExecutor.scheduleAtFixedRate(
      tokenUpdateRunnable, nextRefresh, nextRefresh, TimeUnit.MILLISECONDS)
  }

  private val (principal, keytab) = if (credentialsManagerEnabled) {
    val p = sparkConf.get(SparkHBaseConf.principal, sparkConf.get("spark.yarn.principal", null))
    val k = sparkConf.get(SparkHBaseConf.keytab, sparkConf.get("spark.yarn.keytab", null))
    require(p != null, s"neither ${SparkHBaseConf.principal} nor spark.yarn.principal " +
      s"is configured, this should be configured to make token renewal work")
    require(k != null, s"neither ${SparkHBaseConf.keytab} nor spark.yarn.keytab " +
      s"is configured, this should be configured to make token renewal work")
    (p, k)
  } else {
    (null, null)
  }

  /**
   * Get HBase Token from specified cluster name.
   */
  def getTokenForCluster(conf: Configuration): Array[Byte] = {
    if (!isCredentialsRequired(conf))
      return null

    // var token: Token[_ <: TokenIdentifier] = null
    var serializedToken: Array[Byte] = null
    val identifier = clusterIdentifier(conf)

    val tokenInfoOpt = this.synchronized {
      tokensMap.get(identifier)
    }

    val needNewToken = if (tokenInfoOpt.isDefined) {
      if (isTokenInfoExpired(tokenInfoOpt.get)) {
        // Should not happen if refresh thread works as expected
        logWarning(s"getTokenForCluster: refresh thread may not be working for cluster $identifier")
        true
      } else {
        // token = tokenInfoOpt.get.token
        serializedToken = tokenInfoOpt.get.serializedToken
        logDebug(s"getTokenForCluster: Use existing token for cluster $identifier")
        false
      }
    } else {
      true
    }

    if (needNewToken) {
      logInfo(s"getTokenForCluster: Obtaining new token for cluster $identifier")

      val tokenInfo = getNewToken(conf)
      this.synchronized {
        tokensMap.put(identifier, tokenInfo)
      }

      // token = tokenInfo.token
      serializedToken = tokenInfo.serializedToken

      logInfo(s"getTokenForCluster: Obtained new token with expiration time" +
        s" ${new Date(tokenInfo.expireTime)} and refresh time ${new Date(tokenInfo.refreshTime)} " +
        s"for cluster $identifier")
    }

    serializedToken
  }

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

  private def isCredentialsRequired(conf: Configuration): Boolean =
    credentialsManagerEnabled && conf.get("hbase.security.authentication") == "kerberos"

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
    val kerberosUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
    val tokenInfo = kerberosUgi.doAs(new PrivilegedExceptionAction[TokenInfo] {
      override def run(): TokenInfo = {
        val token = TokenUtil.obtainToken(HBaseConnectionCache.getConnection(conf).connection)
        val tokenIdentifier = token.decodeIdentifier()
        val expireTime = tokenIdentifier.getExpirationDate
        val issueTime = tokenIdentifier.getIssueDate
        val refreshTime = getRefreshTime(issueTime, expireTime)
        new TokenInfo(expireTime, issueTime, refreshTime, conf, token, serializeToken(token))
      }
    })

    UserGroupInformation.getCurrentUser.addToken(tokenInfo.token.getService, tokenInfo.token)
    tokenInfo
  }

  private def clusterIdentifier(conf: Configuration): String = {
    require(conf.get("zookeeper.znode.parent") != null &&
      conf.get("hbase.zookeeper.quorum") != null &&
      conf.get("hbase.zookeeper.property.clientPort") != null)

    conf.get("zookeeper.znode.parent") + "#" +
      conf.get("hbase.zookeeper.quorum") + "#" +
      conf.get("hbase.zookeeper.property.clientPort")
  }
}

object SHCCredentialsManager extends Logging {
  lazy val manager = new  SHCCredentialsManager

  def processShcToken(serializedToken: Array[Byte]): Unit = {
    if (null != serializedToken) {
      val tok = deserializeToken(serializedToken)
      val credentials = new Credentials()
      credentials.addToken(tok.getService, tok)

      logInfo(s"Obtained token with expiration date ${new Date(tok.decodeIdentifier()
        .asInstanceOf[AuthenticationTokenIdentifier].getExpirationDate)}")

      UserGroupInformation.getCurrentUser.addCredentials(credentials)
    }
  }

  private def serializeToken(token: Token[_ <: TokenIdentifier]): Array[Byte] = {
    val dob: DataOutputBuffer = new DataOutputBuffer()
    token.write(dob)
    val dobCopy = new Array[Byte](dob.getLength)
    System.arraycopy(dob.getData, 0, dobCopy, 0, dobCopy.length)
    dobCopy
  }

  private def deserializeToken(tokenBytes: Array[Byte]): Token[_ <: TokenIdentifier] = {
    val byteStream = new ByteArrayInputStream(tokenBytes)
    val dataStream = new DataInputStream(byteStream)
    val token = new Token
    token.readFields(dataStream)
    token
  }
}
