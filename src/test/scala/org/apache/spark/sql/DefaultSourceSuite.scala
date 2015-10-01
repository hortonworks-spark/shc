package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.{SparkContext, Logging}
import  org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

/**
 * Created by zzhang on 9/14/15.
 */
class DefaultSourceSuite extends SHC with Logging {
  val sc = new SparkContext("local", "HBaseTest", conf)
  val sqlContext = new SQLContext(sc)

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("populate table") {
    //createTable(tableName, columnFamilies)
    import sqlContext.implicits._

    val data = (0 to 255).map { i =>
      HBaseRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("full query") {
    val df = withCatalog(catalog)
    df.show
    assert(df.count() == 256)
  }

  test("filtered query") {
    val df = withCatalog(catalog)
    val s = df.filter(($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005")
      .select("col0", "col1")
    s.show
    assert(s.count() == 17)
  }

  test("agg query") {
    val df = withCatalog(catalog)
    df.registerTempTable("table1")
    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    c.show()
    assert(c.collect.apply(0).apply(0).asInstanceOf[Long] == 50)
  }

  test("filtered query1") {
    val df = withCatalog(catalog)
    df.registerTempTable("table1")
    val c = sqlContext.sql("select col1, col0 from table1 where col4 = 5")
    c.show()
    assert(c.count == 1)
  }

  test("complicate filtered query") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      ($"col4" === 1 ||
      $"col4" === 42))
      .select("col0", "col1", "col4")
    s.show
    assert(s.count() == 2)
  }

  test("complicate filtered query1") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") ||
      ($"col4" === 1 ||
        $"col4" === 42))
      .select("col0", "col1", "col4")
    s.show
    assert(s.count() == 17)
  }

  test("String contains filter") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      $"col7".contains("String3"))
    .select("col0", "col1", "col7")
    s.show
    assert(s.count() == 1)
  }

  test("String not contains filter") {
    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      !$"col7".contains("String3"))
      .select("col0", "col1", "col7")
    s.show
    assert(s.count() == 16)
  }

  test("Or filter") {
    val df = withCatalog(catalog)
    val s = df.filter($"col0" <= "row050" || $"col7".contains("String60"))
      .select("col0", "col1", "col7")
    s.show
    assert(s.count() == 52)
  }
}
