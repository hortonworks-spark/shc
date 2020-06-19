package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class HBaseTableCatalogSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging {
  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"table1", "tableCoder":"PrimitiveType"},
                   |"rowkey":"key1:key2",
                   |"columns":{
                   |"col00":{"cf":"rowkey", "col":"key1", "type":"string", "length":"6"},
                   |"col01":{"cf":"rowkey", "col":"key2", "type":"int"},
                   |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                   |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                   |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                   |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                   |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                   |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                   |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"},
                   |"col7":{"cf":"cf7", "col":"col7", "type":"string"}
                   |}
                   |}""".stripMargin

  test("HBaseTableCatalog tableType class variable test") {
    var hbasetablecatalogobject = HBaseTableCatalog(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.nameSpace -> "default",
      HBaseTableCatalog.newTable -> "3"))
    assert(hbasetablecatalogobject.getTableType.equals("hbase"))

    hbasetablecatalogobject = HBaseTableCatalog(Map(HBaseTableCatalog.tableType -> "bigtable", HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.nameSpace -> "default",
      HBaseTableCatalog.newTable -> "3"))
    assert(hbasetablecatalogobject.getTableType.equals("bigtable"))
  }
}
