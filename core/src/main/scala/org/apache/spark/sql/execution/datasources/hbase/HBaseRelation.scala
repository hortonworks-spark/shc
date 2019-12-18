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

import java.io._

import scala.util.control.NonFatal
import scala.xml.XML
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.execution.datasources.hbase.types.{SHCDataType, SHCDataTypeFactory}
import org.apache.spark.util.Utils

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
    relation.createTableIfNotExist()
    relation.insert(data, false)
    relation
  }
}

case class InvalidRegionNumberException(message: String = "", cause: Throwable = null)
              extends Exception(message, cause) 

case class HBaseRelation(
    parameters: Map[String, String],
    userSpecifiedschema: Option[StructType]
  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Logging {

  val timestamp = parameters.get(HBaseRelation.TIMESTAMP).map(_.toLong)
  val minStamp = parameters.get(HBaseRelation.MIN_STAMP).map(_.toLong)
  val maxStamp = parameters.get(HBaseRelation.MAX_STAMP).map(_.toLong)
  val maxVersions = parameters.get(HBaseRelation.MAX_VERSIONS).map(_.toInt)
  val mergeToLatest = parameters.get(HBaseRelation.MERGE_TO_LATEST).map(_.toBoolean).getOrElse(true)
  val restrictive = parameters.getOrElse(HBaseRelation.RESTRICTIVE, HBaseRelation.Restrictive.column)

  val catalog = HBaseTableCatalog(parameters)

  private val wrappedConf = {
    implicit val formats = DefaultFormats
    val hConf = {
      val testConf = sqlContext.sparkContext.conf.getBoolean(SparkHBaseConf.testConf, false)
      if (testConf) {
        SparkHBaseConf.conf
      } else {
        val hBaseConfiguration = parameters.get(HBaseRelation.HBASE_CONFIGURATION).map(
          parse(_).extract[Map[String, String]])

        val cFile = parameters.get(HBaseRelation.HBASE_CONFIGFILE)
        val hBaseConfigFile = {
          var confMap: Map[String, String] = Map.empty
          if (cFile.isDefined) {
            val xmlFile = XML.loadFile(cFile.get)
            (xmlFile \\ "property").foreach(
              x => { confMap += ((x \ "name").text -> (x \ "value").text) })
          }
          confMap
        }

        val conf = HBaseConfiguration.create
        hBaseConfiguration.foreach(_.foreach(e => conf.set(e._1, e._2)))
        hBaseConfigFile.foreach(e => conf.set(e._1, e._2))
        conf
      }
    }
    // task is already broadcast; since hConf is per HBaseRelation (currently), broadcast'ing
    // it again does not help - it actually hurts. When we add support for
    // caching hConf across HBaseRelation, we can revisit broadcast'ing it (with a caching
    // mechanism in place)
    new SerializableConfiguration(hConf)
  }

  def hbaseConf = wrappedConf.value

  val serializedToken = SHCCredentialsManager.manager.getTokenForCluster(hbaseConf)

  def createTableIfNotExist() {
    val cfs = catalog.getColumnFamilies
    val connection = HBaseConnectionCache.getConnection(hbaseConf)
    // Initialize hBase table if necessary
    val admin = connection.getAdmin
    val isNameSpaceExist = try {
      admin.getNamespaceDescriptor(catalog.namespace)
      true
    } catch {
      case e: NamespaceNotFoundException => false
      case NonFatal(e) =>
        logError("Unexpected error", e)
        false
    }
    if (!isNameSpaceExist) {
      admin.createNamespace(NamespaceDescriptor.create(catalog.namespace).build)
    }
    val tName = TableName.valueOf(s"${catalog.namespace}:${catalog.name}")
    // The names of tables which are created by the Examples has prefix "shcExample"
    if (admin.isTableAvailable(tName)
      && tName.toString.startsWith(s"${catalog.namespace}:shcExample")){
      admin.disableTable(tName)
      admin.deleteTable(tName)
    }

    if (!admin.isTableAvailable(tName)) {
      if (catalog.numReg <= 3) {
        throw new InvalidRegionNumberException("Creating a new table should " +
          "specify the number of regions which must be greater than 3.")
      }
      val tableDesc = new HTableDescriptor(tName)
      cfs.foreach { x =>
        val cf = new HColumnDescriptor(x.getBytes())
        logDebug(s"add family $x to ${catalog.name}")
        maxVersions.foreach(v => cf.setMaxVersions(v))
        tableDesc.addFamily(cf)
      }
      val startKey = catalog.shcTableCoder.toBytes(catalog.splitRange._1)
      val endKey = catalog.shcTableCoder.toBytes(catalog.splitRange._2)
      val splitKeys = Bytes.split(startKey, endKey, catalog.numReg - 3)
      admin.createTable(tableDesc, splitKeys)
      val r = connection.getRegionLocator(tName).getAllRegionLocations
      while(r == null || r.size() == 0) {
        logDebug(s"region not allocated")
        Thread.sleep(1000)
      }
      logDebug(s"region allocated $r")
    }

    admin.close()
    connection.close()
  }

  /**
   *
   * @param data DataFrame to write to hbase
   * @param overwrite Overwrite existing values
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, s"${catalog.namespace}:${catalog.name}")
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])

    // This is a workaround for SPARK-21549. After it is fixed, the snippet can be removed.
    val jobConfig = job.getConfiguration
    val tempDir = Utils.createTempDir()
    if (jobConfig.get("mapreduce.output.fileoutputformat.outputdir") == null) {
      jobConfig.set("mapreduce.output.fileoutputformat.outputdir", tempDir.getPath + "/outputDataset")
    }

    val rdd = data.rdd //df.queryExecution.toRdd

    rdd.mapPartitions(iter => {
      SHCCredentialsManager.processShcToken(serializedToken)
      iter.map(convertToPut(catalog.getRowKey))
    }).saveAsNewAPIHadoopDataset(jobConfig)
  }

  private def convertToPut(rkFields: Seq[Field] )(row: Row): (ImmutableBytesWritable, Put) = {
    val rkIdxedFields = rkFields.map{ case x =>
      (schema.fieldIndex(x.colName), x)
    }
    val colsIdxedFields = schema
      .fieldNames
      .partition( x => rkFields.map(_.colName).contains(x))
      ._2.map(x => (schema.fieldIndex(x), catalog.getField(x)))
    val coder: SHCDataType = catalog.shcTableCoder

    // construct bytes for row key
    val rBytes: Array[Byte] =
      if (isComposite()) {
        val rowBytes = coder.encodeCompositeRowKey(rkIdxedFields, row)

        val rLen = rowBytes.foldLeft(0) { case (x, y) =>
          x + y.length
        }
        val rBytes = new Array[Byte](rLen)
        var offset = 0
        rowBytes.foreach { x =>
          System.arraycopy(x, 0, rBytes, offset, x.length)
          offset += x.length
        }
        rBytes
      } else {
        rkIdxedFields.map { case (x, y) =>
          SHCDataTypeFactory.create(y).toBytes(row(x))
        }.head
      }

    // add timestamp if defined for whole table
    val put: Put = timestamp.fold(new Put(rBytes))(new Put(rBytes, _))

    colsIdxedFields.foreach { case (index, field) =>
      val dataType = SHCDataTypeFactory.create(field)
      def addColumnWithTime(col: String)(ts: Long, value: Any) = if(value != null) {
        put.addColumn(coder.toBytes(field.cf), coder.toBytes(col), ts, dataType.toBytes(value))
      }
      def addColumn(col: String, value: Any) = if(value != null) {
        put.addColumn(coder.toBytes(field.cf), coder.toBytes(col), dataType.toBytes(value))
      }
      field.dt match {
        case MapType(keyType, valueType, _) =>
          keyType match {
            case StringType =>
              valueType match {
                case MapType(LongType, _, _) =>
                  row(index).asInstanceOf[Map[String, Map[Long, Any]]]
                    .foreach{ case (col, versions) =>
                      versions.foreach((addColumnWithTime(col) _).tupled)
                    }
                case _ =>
                  row(index).asInstanceOf[Map[String, Any]]
                    .foreach((addColumn _).tupled)
              }
            case LongType =>
              row(index).asInstanceOf[Map[Long, Any]]
                .foreach((addColumnWithTime(field.col) _).tupled)
          }
        case _ =>
          addColumn(field.col, row(index))
      }
    }
    (new ImmutableBytesWritable, put)
  }

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

  // Tell Spark about filters that has not handled by HBase as opposed to returning all the filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(!HBaseFilter.buildFilter(_, this).handled)
  }

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

  object Restrictive {
    val none = "NONE"
    val family = "FAMILY"
    val column = "COLUMN"
  }

  val RESTRICTIVE = "restrictive"
  val TIMESTAMP = "timestamp"
  val MIN_STAMP = "minStamp"
  val MAX_STAMP = "maxStamp"
  val MERGE_TO_LATEST = "mergeToLatest"
  val MAX_VERSIONS = "maxVersions"
  val HBASE_CONFIGURATION = "hbaseConfiguration"
  // HBase configuration file such as HBase-site.xml, core-site.xml
  val HBASE_CONFIGFILE = "hbaseConfigFile"
}
