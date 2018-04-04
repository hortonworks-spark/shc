package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

class HBaseStreamSink(options: Map[String, String]) extends Sink with Logging {

  val defaultFormat = "org.apache.spark.sql.execution.datasources.hbase"
  // String with HBaseTableCatalog.tableCatalog
  private val hBaseCatalog =
    options.get("hbasecatalog").map(_.toString).getOrElse("")

  if (hBaseCatalog.isEmpty)
    throw new IllegalArgumentException(
      "hbasecatalog - variable must be specified in option")

  private val newTableCount =
    options.get("newtablecount").map(_.toString).getOrElse("5")

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {

    /** As per SPARK-16020 arbitrary transformations are not supported, but
      * converting to an RDD allows us to do magic.
      */
    val df = data.sparkSession.createDataFrame(data.rdd, data.schema)
    df.write
      .options(Map(HBaseTableCatalog.tableCatalog -> hBaseCatalog,
                   HBaseTableCatalog.newTable -> newTableCount))
      .format(defaultFormat)
      .save()
  }
}

/**
  * In option must be specified string with HBaseTableCatalog.tableCatalog
  * {{{
  *   inputDF.
  *    writeStream.  *
  *    format("org.apache.spark.sql.execution.datasources.hbase.HBaseStreamSinkProvider").
  *    option("checkpointLocation", checkPointProdPath).
  *    option("hbasecatalog", catalog).
  *    outputMode(OutputMode.Update()).
  *    trigger(Trigger.ProcessingTime(30.seconds)).
  *    start
  * }}}
  */
class HBaseStreamSinkProvider
    extends StreamSinkProvider
    with DataSourceRegister {
  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    new HBaseStreamSink(parameters)
  }

  def shortName(): String = "hbase"
}
