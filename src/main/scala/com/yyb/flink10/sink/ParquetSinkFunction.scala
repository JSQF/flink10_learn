package com.yyb.flink10.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-25
  * @Time 21:47
  */
class ParquetSinkFunction[IN](val path: String, val schema: String, val compressionCodecName: CompressionCodecName) extends RichSinkFunction[IN]{
  var parquetWriter: ParquetWriter[IN] = null

  override def close(): Unit = {
    parquetWriter.close()
  }

  override def invoke(value: IN, context: SinkFunction.Context[_]): Unit = {
    parquetWriter.write(value)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ctx = getRuntimeContext
    parquetWriter
  }
}
