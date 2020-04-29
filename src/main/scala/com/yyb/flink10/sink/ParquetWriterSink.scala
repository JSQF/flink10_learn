package com.yyb.flink10.sink

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.fs.Path
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class ParquetWriterSink[IN](val path: String, val schema: String, val compressionCodecName: CompressionCodecName) extends RichSinkFunction[IN] {
  var parquetWriter: ParquetWriter[IN] = null

  override def open(parameters: Configuration): Unit = {
    parquetWriter =  AvroParquetWriter.builder[IN](new Path(path))
      .withSchema(new Schema.Parser().parse(schema))
      .withCompressionCodec(compressionCodecName)
//      .withPageSize(config.pageSize)
//      .withRowGroupSize(config.blockSize)
//      .withDictionaryEncoding(config.enableDictionary)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
//      .withValidation(config.validating)
        .withDataModel(ReflectData.get)
      .build()
  }

  override def close(): Unit = {
    parquetWriter.close()
  }

  override def invoke(value: IN, context: SinkFunction.Context[_]): Unit = {
    parquetWriter.write(value)
  }
}
