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

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ipc.RpcControllerFactory
import org.apache.hadoop.hbase.security.{User, UserProvider}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Utils {

  /**
   * Parses the hbase field to it's corresponding
   * scala type which can then be put into a Spark GenericRow
   * which is then automatically converted by Spark.
   */
  def hbaseFieldToScalaType(
      f: Field,
      src: HBaseType,
      offset: Int,
      length: Int): Any = {
    if (f.sedes.isDefined) {
      // If we already have sedes defined , use it.
      f.sedes.get.deserialize(src, offset, length)
    } else if (f.exeSchema.isDefined) {
      // println("avro schema is defined to do deserialization")
      // If we have avro schema defined, use it to get record, and then covert them to catalyst data type
      val m = AvroSedes.deserialize(src, f.exeSchema.get)
      // println(m)
      val n = f.avroToCatalyst.map(_(m))
      n.get
    } else  {
      // Fall back to atomic type
      f.dt match {
        case BooleanType => toBoolean(src, offset)
        case ByteType => src(offset)
        case DoubleType => Bytes.toDouble(src, offset)
        case FloatType => Bytes.toFloat(src, offset)
        case IntegerType => Bytes.toInt(src, offset)
        case LongType => Bytes.toLong(src, offset)
        case ShortType => Bytes.toShort(src, offset)
        case StringType => toUTF8String(src, offset, length)
        case BinaryType =>
          val newArray = new Array[Byte](length)
          System.arraycopy(src, offset, newArray, 0, length)
          newArray
        case _ => throw new Exception(s"unsupported data type ${f.dt}")
      }
    }
  }

  // convert input to data type
  def toBytes(input: Any, field: Field): Array[Byte] = {
    if (field.sedes.isDefined) {
      field.sedes.get.serialize(input)
    } else if (field.schema.isDefined) {
      // Here we assume the top level type is structType
      val record = field.catalystToAvro(input)
      AvroSedes.serialize(record, field.schema.get)
    } else {
      input match {
        case data: Boolean => Bytes.toBytes(data)
        case data: Byte => Array(data)
        case data: Array[Byte] => data
        case data: Double => Bytes.toBytes(data)
        case data: Float => Bytes.toBytes(data)
        case data: Int => Bytes.toBytes(data)
        case data: Long => Bytes.toBytes(data)
        case data: Short => Bytes.toBytes(data)
        case data: UTF8String => data.getBytes
        case data: String => Bytes.toBytes(data)
          //Bytes.toBytes(input.asInstanceOf[String])//input.asInstanceOf[UTF8String].getBytes
        case _ => throw new Exception(s"unsupported data type ${field.dt}") //TODO
      }
    }
  }

  def toBoolean(input: HBaseType, offset: Int): Boolean = {
    input(offset) != 0
  }

  def toUTF8String(input: HBaseType, offset: Int, length: Int): UTF8String = {
    UTF8String.fromBytes(input.slice(offset, offset + length))
  }

  /**
    * scala doesn't support lambda conversion for functional interfaces as Java does,
    * so solve this by implementing a java.util.function.Function explicitly
    */
  object FuncConverter {
    implicit def scalaFuncToJava[From, To](f: (From) => To): java.util.function.Function[From, To] = {
      new java.util.function.Function[From, To] {
        override def apply(input: From): To = f(input)
      }
    }
  }

  /**
    * A current map of Spark-HBase connections. Key is HBaseConnectionKey.
    */
  val connectionMap = new ConcurrentHashMap[HBaseConnectionKey, Connection]()

  def getConnection(key: HBaseConnectionKey): Connection = {
    ConnectionFactory.createConnection(key.conf)
  }

  def removeAllConnections() = {
    connectionMap.foreach{
       x => x._2.close()
    }
  }
}

/**
  * Denotes a unique key to an HBase Connection instance.
  * Please refer to 'org.apache.hadoop.hbase.client.HConnectionKey'.
  *
  * In essence, this class captures the properties in Configuration
  * that may be used in the process of establishing a connection.
  *
  */
case class HBaseConnectionKey(conf: Configuration) extends Logging {
  val CONNECTION_PROPERTIES: Array[String] = Array[String](
    HConstants.ZOOKEEPER_QUORUM,
    HConstants.ZOOKEEPER_ZNODE_PARENT,
    HConstants.ZOOKEEPER_CLIENT_PORT,
    HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
    HConstants.HBASE_CLIENT_PAUSE,
    HConstants.HBASE_CLIENT_RETRIES_NUMBER,
    HConstants.HBASE_RPC_TIMEOUT_KEY,
    HConstants.HBASE_META_SCANNER_CACHING,
    HConstants.HBASE_CLIENT_INSTANCE_ID,
    HConstants.RPC_CODEC_CONF_KEY,
    HConstants.USE_META_REPLICAS,
    RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY)

  var username: String = _
  var m_properties = mutable.HashMap.empty[String, String]
  if (conf != null) {
    for (property <- CONNECTION_PROPERTIES) {
      val value: String = conf.get(property)
      if (value != null) {
        m_properties.+=((property, value))
      }
    }
  }
  try {
    val provider: UserProvider = UserProvider.instantiate(conf)
    val currentUser: User = provider.getCurrent
    if (currentUser != null) {
      username = currentUser.getName
    }
  }
  catch {
    case e: IOException => {
      logWarning("Error obtaining current user, skipping username in HBaseConnectionKey", e)
    }
  }

  // make 'properties' immutable
  val properties = m_properties.toMap

  override def hashCode: Int = {
    val prime: Int = 31
    var result: Int = 1
    if (username != null) {
      result = username.hashCode
    }
    for (property <- CONNECTION_PROPERTIES) {
      val value: Option[String] = properties.get(property)
      if (value.isDefined) {
        result = prime * result + value.hashCode
      }
    }
    result
  }


  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass ne obj.getClass) return false
    val that: HBaseConnectionKey = obj.asInstanceOf[HBaseConnectionKey]
    if (this.username != null && !(this.username == that.username)) {
      return false
    }
    else if (this.username == null && that.username != null) {
      return false
    }
    if (this.properties == null) {
      if (that.properties != null) {
        return false
      }
    }
    else {
      if (that.properties == null) {
        return false
      }
      var flag: Boolean = true
      for (property <- CONNECTION_PROPERTIES) {
        val thisValue: Option[String] = this.properties.get(property)
        val thatValue: Option[String] = that.properties.get(property)
        flag = true
        if (thisValue eq thatValue) {
          flag = false //continue, so make flag false
        }
        if (flag && (thisValue == null || !(thisValue == thatValue))) {
          return false
        }
      }
    }
    true
  }

  override def toString: String = {
    "HBaseConnectionKey{" + "properties=" + properties + ", username='" + username + '\'' + '}'
  }
}

