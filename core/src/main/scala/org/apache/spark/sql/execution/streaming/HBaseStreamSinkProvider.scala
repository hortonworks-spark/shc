package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.execution.datasources.hbase.{
  HBaseTableCatalog,
  Logging
}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

class HBaseStreamSink(parameters: Map[String, String])
    extends Sink
    with Logging {

  private val defaultFormat = "org.apache.spark.sql.execution.datasources.hbase"

  private val hbaseOptionPrefix = "hbase."

  private val hbaseSettings = parameters.filterKeys(
    _.toLowerCase matches hbaseOptionPrefix + "*") map {
    case (k, v) => (k.replace(hbaseOptionPrefix, ""), v)
  }

  private val hBaseCatalog =
    hbaseSettings
      .get(HBaseTableCatalog.tableCatalog)
      .map(_.toString)
      .getOrElse("")

  if (hBaseCatalog.isEmpty)
    throw new IllegalArgumentException(
      "hbase.catalog - must be specified in option")

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {

    /** As per SPARK-16020 arbitrary transformations are not supported, but
      * converting to an RDD allows us to do magic.
      */
    val df = data.sparkSession.createDataFrame(data.rdd, data.schema)

    df.write
      .options(hbaseSettings)
      .format(defaultFormat)
      .save()
  }
}

/**
  * In option must be specified string with HBaseTableCatalog.tableCatalog
  * {{{
  *   inputDF.
  *    writeStream.
  *    format("hbase").
  *    option("checkpointLocation", checkPointProdPath).
  *    options(Map("hbase.schema_array"->schema_array,"hbase.schema_record"->schema_record, hbase..catalog->catalog)).
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
