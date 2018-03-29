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

package org.apache.spark.sql;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.sql.execution.datasources.hbase.SparkHBaseConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Ignore
public class AvroKeySourceSuite {

  private static final TableName TABLE_NAME = TableName.valueOf("TEST_TABLE");
  private static final String COLUMN_FAMILY = "COL_FAMILY";
  private static final String COLUMN_QUALIFIER = "COL_QUALIFIER";

  private static final String KEY1 = "KEY1";
  private static final String KEY2 = "KEY2";

  private final HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();

  private HBaseCluster hbase;
  private SparkSession sparkSession;

  @Before
  public void setUp() throws Exception {
    hbase = hBaseTestingUtility.startMiniCluster();
    SparkHBaseConf.conf_$eq(hbase.getConf());
    sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestHBaseAvroKey")
      .config(SparkHBaseConf.testConf(), "true")
      .getOrCreate();
  }

  @After
  public void tearDown() throws Exception {
    hBaseTestingUtility.shutdownMiniCluster();
    sparkSession.stop();
  }

  public static Comparator<Row> rowComparator = new Comparator<Row>() {
    @Override
    public int compare(Row a, Row b) {
      return a.getStruct(1).getStruct(1).getInt(0) - b.getStruct(1).getStruct(1).getInt(0);
    }
  };

  @Test
  public void testAvroKey() throws Exception {
    hBaseTestingUtility.createTable(TABLE_NAME, COLUMN_FAMILY);
    writeDataToHBase(hbase);

    // Assert contents look as expected.
    Dataset<Row> df = sparkSession.sqlContext().read()
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .options(getHBaseSourceOptions()).load();
    assertEquals(2, df.count());
    df.show();
    Row[] rows = (Row[])df.collect();

    // Arrays.sort(rows, (a, b) -> {
    //   return a.getStruct(1).getStruct(1).getInt(0) - b.getStruct(1).getStruct(1).getInt(0);
    // });

    Arrays.sort(rows, rowComparator);

    // Note that the key is duplicated in the hbase key and in the object
    assertEquals(KEY1, rows[0].getStruct(0).getString(0));
    assertEquals(KEY1, rows[0].getStruct(1).getStruct(0).getString(0));
    assertEquals(5, rows[0].getStruct(1).getStruct(1).getInt(0));

    assertEquals(KEY2, rows[1].getStruct(0).getString(0));
    assertEquals(KEY2, rows[1].getStruct(1).getStruct(0).getString(0));
    assertEquals(7, rows[1].getStruct(1).getStruct(1).getInt(0));
  }

  private static void putRecord(HTable table, GenericRecord object) throws Exception {
    // The rowKey doesn't actually matter too much.
    byte[] keyBytes = avroEncoderFunc((GenericRecord) object.get("key"));
    byte[] recordBytes = avroEncoderFunc(object);
    Put p = new Put(keyBytes);
    p.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_QUALIFIER), recordBytes);
    table.put(p);
  }

  private static void writeDataToHBase(HBaseCluster hbase) throws Exception {
    // Write some data directly to it
    GenericRecord record1 = getRecord(KEY1, 5);
    GenericRecord record2 = getRecord(KEY2, 7);
    //HTable testTable = new HTable(hbase.getConf(), TABLE_NAME);
    HTable testTable = null;
    putRecord(testTable, record1);
    putRecord(testTable, record2);
  }

  private static Map<String, String> getHBaseSourceOptions() {
    String hbaseCatalog = "{\"table\": {\"namespace\": \"default\", \"name\": \"TEST_TABLE\", \"tableCoder\":\"PrimitiveType\"}," +
        "\"rowkey\": \"key\", \"columns\": {"
        + "\"key\": {\"cf\": \"rowkey\", \"col\": \"key\", \"avro\": \"keySchema\"},"
        + "\"value\": {\"cf\": \"" + COLUMN_FAMILY + "\", \"col\": \"" + COLUMN_QUALIFIER + "\", \"avro\": \"avroSchema\"}"
        + "}}";
    Map<String, String> hbaseOptions = new HashMap<>();
    hbaseOptions.put(HBaseTableCatalog.tableCatalog(), hbaseCatalog);
    System.out.println("keySchema: " + getSchema().getField("key").schema().toString());
    System.out.println("avroSchema: " + getSchema().toString());
    hbaseOptions.put("keySchema", getSchema().getField("key").schema().toString());
    hbaseOptions.put("avroSchema", getSchema().toString());
    return hbaseOptions;
  }

  private static Schema getSchema() {
    return SchemaBuilder.record("TestKeyedRecord").namespace("test")
        .fields()
        .name("key").type().record("Key").namespace("test")
        .fields().name("keyValue").type().stringType().noDefault()
        .endRecord().noDefault()
        .name("data").type().record("Data").namespace("test")
        .fields().name("dataValue").type().intType().noDefault()
        .endRecord().noDefault()
        .endRecord();
  }

  private static GenericRecord getRecord(String key, int value) {
    Schema keySchema = getSchema().getField("key").schema();
    GenericRecord keyRecord = new GenericData.Record(keySchema);
    keyRecord.put("keyValue", key);

    Schema dataSchema = getSchema().getField("data").schema();
    GenericRecord dataRecord = new GenericData.Record(dataSchema);
    dataRecord.put("dataValue", value);

    GenericRecord record = new GenericData.Record(getSchema());
    record.put("key", keyRecord);
    record.put("data", dataRecord);
    return record;
  }

  public static byte[] avroEncoderFunc(GenericRecord record) {
    Schema schema = record.getSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    try {
      datumWriter.write(record, encoder);
    } catch (IOException e) {
      throw new RuntimeException("Problem serializing " + record.toString(), e);
    }
    return baos.toByteArray();
  }

}
