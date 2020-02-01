package org.apache.spark.sql.execution.streaming

import java.util.Locale

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class HBaseStreamSink(sqlContext: SQLContext,
                      parameters: Map[String, String],
                      partitionColumns: Seq[String],
                      outputMode: OutputMode)
    extends Sink
    with Logging {

  @volatile private var latestBatchId = -1L

  private val defaultFormat = "org.apache.spark.sql.execution.datasources.hbase"
  private val prefix = "hbase."

  private val specifiedHBaseParams = parameters
    .keySet
    .filter(_.toLowerCase(Locale.ROOT).startsWith(prefix))
    .map { k => k.drop(prefix.length).toString -> parameters(k) }
    .toMap

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      // use a local variable to make sure the map closure doesn't capture the whole DataFrame
      val schema = data.schema
      val res = data.queryExecution.toRdd.mapPartitions { rows =>
        val converter = CatalystTypeConverters.createToScalaConverter(schema)
        rows.map(converter(_).asInstanceOf[Row])
      }

      val df = sqlContext.sparkSession.createDataFrame(res, schema)
      df.write
        .options(specifiedHBaseParams)
        .format(defaultFormat)
        .save()
    }
  }
}

/**
  * In option must be specified string with HBaseTableCatalog.tableCatalog
  * {{{
  *   inputDF.
  *    writeStream.
  *    format("hbase").
  *    option("checkpointLocation", checkPointProdPath).
  *    options(Map("hbase.schema_array"->schema_array,"hbase.schema_record"->schema_record, hbase.catalog->catalog)).
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
    new HBaseStreamSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  def shortName(): String = "hbase"
}
