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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.util.control.NonFatal

/**
 * val people = sqlContext.read.format("hbase").load("people")
 */
private[sql] class DefaultSource extends RelationProvider with CreatableRelationProvider {//with DataSourceRegister {

  //override def shortName(): String = "hbase"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    HBaseRelation(parameters, None)(sqlContext)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val relation = HBaseRelation(parameters, Some(data.schema))(sqlContext)
    relation.createTable()
    relation.insert(data, false)
    relation
  }
}

case class HBaseRelation(
    parameters: Map[String, String],
    userSpecifiedschema: Option[StructType]
  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Logging {

  val timestamp = parameters.get(HBaseRelation.TIMESTAMP).map(_.toLong)
  val minStamp = parameters.get(HBaseRelation.MIN_STAMP).map(_.toLong)
  val maxStamp = parameters.get(HBaseRelation.MAX_STAMP).map(_.toLong)
  val maxVersions = parameters.get(HBaseRelation.MAX_VERSIONS).map(_.toInt)

  @transient implicit val formats = DefaultFormats
  val hBaseConfiguration = parameters.get(HBaseRelation.HBASE_CONFIGURATION).map(parse(_).extract[Map[String, String]])

  def createTable() {
    if (catalog.numReg > 3) {
      val tName = TableName.valueOf(catalog.name)
      val cfs = catalog.getColumnFamilies
      val connection = ConnectionFactory.createConnection(hbaseConf)
      // Initialize hBase table if necessary
      val admin = connection.getAdmin()

      // The names of tables which are created by the Examples has prefix "shcExample"
      if (admin.isTableAvailable(tName) && tName.toString.startsWith("shcExample")){
        admin.disableTable(tName)
        admin.deleteTable(tName)
      }

      if (!admin.isTableAvailable(tName)) {
        val tableDesc = new HTableDescriptor(tName)
        cfs.foreach { x =>
         val cf = new HColumnDescriptor(x.getBytes())
          logDebug(s"add family $x to ${catalog.name}")
          tableDesc.addFamily(cf)
        }
        val startKey = Bytes.toBytes("aaaaaaa");
        val endKey = Bytes.toBytes("zzzzzzz");
        val splitKeys = Bytes.split(startKey, endKey, catalog.numReg - 3);
        admin.createTable(tableDesc, splitKeys)
        val r = connection.getRegionLocator(TableName.valueOf(catalog.name)).getAllRegionLocations
        while(r == null || r.size() == 0) {
          logDebug(s"region not allocated")
          Thread.sleep(1000)
        }
        logDebug(s"region allocated $r")

      }
      admin.close()
      connection.close()
    }
  }

  /**
   *
   * @param data
   * @param overwrite
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val jobConfig: JobConf = new JobConf(hbaseConf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, catalog.name)
    var count = 0
    val rkFields = catalog.getRowKey
    val rkIdxedFields = rkFields.map{ case x =>
      (schema.fieldIndex(x.colName), x)
    }
    val colsIdxedFields = schema
      .fieldNames
      .partition( x => rkFields.map(_.colName).contains(x))
      ._2.map(x => (schema.fieldIndex(x), catalog.getField(x)))
    val rdd = data.rdd //df.queryExecution.toRdd
    def convertToPut(row: Row) = {
      // construct bytes for row key
      val rowBytes = rkIdxedFields.map { case (x, y) =>
        Utils.toBytes(row(x), y)
      }
      val rLen = rowBytes.foldLeft(0) { case (x, y) =>
        x + y.length
      }
      val rBytes = new Array[Byte](rLen)
      var offset = 0
      rowBytes.foreach { x =>
        System.arraycopy(x, 0, rBytes, offset, x.length)
        offset += x.length
      }
      val put = timestamp.fold(new Put(rBytes))(new Put(rBytes, _))

      colsIdxedFields.foreach { case (x, y) =>
        val b = Utils.toBytes(row(x), y)
        put.addColumn(Bytes.toBytes(y.cf), Bytes.toBytes(y.col), b)
      }
      count += 1
      (new ImmutableBytesWritable, put)
    }
    rdd.map(convertToPut(_)).saveAsHadoopDataset(jobConfig)
  }
  val catalog = HBaseTableCatalog(parameters)

  val df: DataFrame = null

  val testConf = sqlContext.sparkContext.conf.getBoolean(SparkHBaseConf.testConf, false)

  @transient val  hConf = {
    if (testConf) {
      SparkHBaseConf.conf
    } else {
      val conf = HBaseConfiguration.create
      hBaseConfiguration.foreach(_.foreach(e => conf.set(e._1, e._2)))
      conf
    }
  }
  val wrappedConf = sqlContext.sparkContext.broadcast(new SerializableConfiguration(hConf))
  def hbaseConf = wrappedConf.value.value

  def rows = catalog.row

  def singleKey = {
    rows.fields.size == 1
  }

  def getField(name: String): Field = {
    catalog.getField(name)
  }

  // check whether the column is the first key in the rowkey
  def isPrimaryKey(c: String): Boolean = {
    val f1 = catalog.getRowKey(0)
    val f2 = getField(c)
    f1 == f2
  }

  def isComposite(): Boolean = {
    catalog.getRowKey.size > 1
  }
  def isColumn(c: String): Boolean = {
    !catalog.getRowKey.map(_.colName).contains(c)
  }

  // Return the key that can be used as partition keys, which satisfying two conditions:
  // 1: it has to be the row key
  // 2: it has to be sequentially sorted without gap in the row key
  def getRowColumns(c: Seq[Field]): Seq[Field] = {
    catalog.getRowKey.zipWithIndex.filter { x =>
      c.contains(x._1)
    }.zipWithIndex.filter { x =>
      x._1._2 == x._2
    }.map(_._1._1)
  }

  def getIndexedProjections(requiredColumns: Array[String]): Seq[(Field, Int)] = {
    requiredColumns.map(catalog.sMap.getField(_)).zipWithIndex
  }
  // Retrieve all columns we will return in the scanner
  def splitRowKeyColumns(requiredColumns: Array[String]): (Seq[Field], Seq[Field]) = {
    val (l, r) = requiredColumns.map(catalog.sMap.getField(_)).partition(_.cf == HBaseTableCatalog.rowKey)
    (l, r)
  }

  override val schema: StructType = userSpecifiedschema.getOrElse(catalog.toDataType)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new HBaseTableScanRDD(this, requiredColumns, filters)
  }
}

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }

  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }
}

object HBaseRelation {

  val TIMESTAMP = "timestamp"
  val MIN_STAMP = "minStamp"
  val MAX_STAMP = "maxStamp"
  val MAX_VERSIONS = "maxVersions"
  val HBASE_CONFIGURATION = "hbaseConfiguration"

}
