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

import org.apache.avro.Schema
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable



// The definition of each column cell, which may be composite type
case class Field(
    colName: String,
    cf: String,
    col: String,
    sType: Option[String] = None,
    avroSchema: Option[String] = None,
    val sedes: Option[Sedes]= None,
    private val len: Int = -1) extends Logging{
  val isRowKey = cf == HBaseTableCatalog.rowKey
  var start: Int = _
  def schema: Option[Schema] = avroSchema.map { x =>
    logDebug(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }

  lazy val exeSchema = schema

  // converter from avro to catalyst structure
  lazy val avroToCatalyst: Option[Any => Any] = {
    schema.map(SchemaConverters.createConverterToSQL(_))
  }

  // converter from catalyst to avro
  lazy val catalystToAvro: (Any) => Any ={
    SchemaConverters.createConverterToAvro(dt, colName, "recordNamespace")
  }

  val dt = {
    sType.map(DataTypeParser.parse(_)).getOrElse{
      schema.map{ x=>
        SchemaConverters.toSqlType(x).dataType
      }.get
    }
  }

  val length: Int = {
    if (len == -1) {
      dt match {
        case BinaryType | StringType => -1
        case BooleanType => Bytes.SIZEOF_BOOLEAN
        case ByteType => 1
        case DoubleType => Bytes.SIZEOF_DOUBLE
        case FloatType => Bytes.SIZEOF_FLOAT
        case IntegerType => Bytes.SIZEOF_INT
        case LongType => Bytes.SIZEOF_LONG
        case ShortType => Bytes.SIZEOF_SHORT
        case _ => -1
      }
    } else {
      len
    }

  }

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      colName == that.colName && cf == that.cf && col == that.col
    case _ => false
  }
}

// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3
case class RowKey(k: String) {
  val keys = k.split(":")
  var fields: Seq[Field] = _
  var varLength = false
  def length = {
    fields.foldLeft(0) { case (x, y) =>
      val yLen = if (y.length == -1) {
        MaxLength
      } else {
        y.length
      }
      x + y.length
    }
  }
}
// The map between the column presented to Spark and the HBase field
case class SchemaMap(map: mutable.HashMap[String, Field]) {
  def toFields = map.map { case (name, field) =>
    StructField(name, field.dt)
  }.toSeq

  def fields = map.values

  def getField(name: String) = map(name)
}


// The definition of HBase and Relation relation schema
case class HBaseTableCatalog(
    val namespace: String,
    val name: String,
    row: RowKey,
    sMap: SchemaMap,
    val numReg: Int) extends Logging {
  def toDataType = StructType(sMap.toFields)
  def getField(name: String) = sMap.getField(name)
  def getRowKey: Seq[Field] = row.fields
  def getPrimaryKey= row.keys(0)
  def getColumnFamilies = {
    sMap.fields.map(_.cf).filter(_ != HBaseTableCatalog.rowKey)
  }

  def initRowKey = {
    val fields = sMap.fields.filter(_.cf == HBaseTableCatalog.rowKey)
    row.fields = row.keys.flatMap(n => fields.find(_.col == n))
    // We only allowed there is one key at the end that is determined at runtime.
    if (row.fields.reverse.tail.filter(_.length == -1).isEmpty) {
      var start = 0
      row.fields.foreach { f =>
        f.start = start
        start += f.length
      }
    } else {
      throw new Exception("Only the last dimension of " +
        "RowKey is allowed to have varied length")
    }
  }
  initRowKey
}

object HBaseTableCatalog {
  val newTable = "newtable"
  // The json string specifying hbase catalog information
  val tableCatalog = "catalog"
  // The row key with format key1:key2 specifying table row key
  val rowKey = "rowkey"
  // The key for hbase table whose value specify namespace and table name
  val table = "table"
  // The namespace of hbase table
  val nameSpace = "namespace"
  // The name of hbase table
  val tableName = "name"
  // The name of columns in hbase catalog
  val columns = "columns"
  val cf = "cf"
  val col = "col"
  val `type` = "type"
  // the name of avro schema json string
  val avro = "avro"
  val delimiter: Byte = 0
  val sedes = "sedes"
  val length = "length"
  /**
   * User provide table schema definition
   * {"tablename":"name", "rowkey":"key1:key2",
   * "columns":{"col1":{"cf":"cf1", "col":"col1", "type":"type1"},
   * "col2":{"cf":"cf2", "col":"col2", "type":"type2"}}}
   *  Note that any col in the rowKey, there has to be one corresponding col defined in columns
   */
  def apply(parameters: Map[String, String]): HBaseTableCatalog = {
    //  println(jString)
    val jString = parameters(tableCatalog)
    val map= parse(jString).values.asInstanceOf[Map[String,_]]
    val tableMeta = map.get(table).get.asInstanceOf[Map[String, _]]
    val nSpace = tableMeta.get(nameSpace).getOrElse("default").asInstanceOf[String]
    val tName = tableMeta.get(tableName).get.asInstanceOf[String]
    val cIter = map.get(columns).get.asInstanceOf[Map[String, Map[String, String]]].toIterator
    val schemaMap = mutable.HashMap.empty[String, Field]
    cIter.foreach { case (name, column)=>
      val sd = {
        column.get(sedes).asInstanceOf[Option[String]].map( n =>
          Class.forName(n).newInstance().asInstanceOf[Sedes]
        )
      }
      val len = column.get(length).map(_.toInt).getOrElse(-1)
      val sAvro = column.get(avro).map(parameters(_))
      val f = Field(name, column.getOrElse(cf, rowKey),
        column.get(col).get,
        column.get(`type`),
        sAvro, sd, len)
      schemaMap.+= ((name, f))
    }
    val numReg = parameters.get(newTable).map(x => x.toInt).getOrElse(0)
    val rKey = RowKey(map.get(rowKey).get.asInstanceOf[String])
    HBaseTableCatalog(nSpace, tName, rKey, SchemaMap(schemaMap), numReg)
  }

  def main(args: Array[String]) {
    val complex = s"""MAP<int, struct<varchar:string>>"""
    val schema =
      s"""{"namespace": "example.avro",
         |   "type": "record",      "name": "User",
         |    "fields": [      {"name": "name", "type": "string"},
         |      {"name": "favorite_number",  "type": ["int", "null"]},
         |        {"name": "favorite_color", "type": ["string", "null"]}      ]    }""".stripMargin

    val catalog = s"""{
            |"table":{"namespace":"default", "name":"htable"},
            |"rowkey":"key1:key2",
            |"columns":{
              |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
              |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
              |"col3":{"cf":"cf1", "col":"col1", "avro":"schema1"},
              |"col4":{"cf":"cf1", "col":"col2", "type":"binary"},
              |"col5":{"cf":"cf1", "col":"col3", "type":"double", "sedes":"org.apache.spark.sql.execution.datasources.hbase.DoubleSedes"},
              |"col6":{"cf":"cf1", "col":"col4", "type":"$complex"}
            |}
          |}""".stripMargin
    val parameters = Map("schema1"->schema, tableCatalog->catalog)
    val t = HBaseTableCatalog(parameters)
    val d = t.toDataType
    println(d)

    val sqlContext: SQLContext = null
  }
}
