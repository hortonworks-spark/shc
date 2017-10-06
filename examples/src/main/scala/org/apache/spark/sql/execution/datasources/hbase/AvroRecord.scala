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

package org.apache.spark.sql.execution.datasources.hbase.examples

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.execution.datasources.hbase.types.{AvroSerde, SchemaConverters}

object AvroRecord {
  def main(args: Array[String]) {
    //Test avro to schema converterBasic setup
    val schemaString =
      """{"namespace": "example.avro",
        |   "type": "record", "name": "User",
        |    "fields": [ {"name": "name", "type": "string"},
        |      {"name": "favorite_number",  "type": ["int", "null"]},
        |        {"name": "favorite_color", "type": ["string", "null"]} ] }""".stripMargin

    val avroSchema: Schema = {
      val p = new Schema.Parser
      p.parse(schemaString)
    }
    val user1 = new GenericData.Record(avroSchema)
    user1.put("name", "Alyssa")
    user1.put("favorite_number", 256)

    val user2 = new GenericData.Record(avroSchema)
    user2.put("name", "Ben")
    user2.put("favorite_number", 7)
    user2.put("favorite_color", "red")

    val sqlUser1 = SchemaConverters.createConverterToSQL(avroSchema)(user1)
    println(sqlUser1)
    val schema = SchemaConverters.toSqlType(avroSchema)
    println(s"\nSqlschema: $schema")
    val avroUser1 = SchemaConverters.createConverterToAvro(schema.dataType, "avro", "example.avro")(sqlUser1)
    val avroByte = AvroSerde.serialize(avroUser1, avroSchema)
    val avroUser11 = AvroSerde.deserialize(avroByte, avroSchema)
    println(s"$avroUser1")
  }
}
