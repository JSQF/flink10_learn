package com.yyb.flink10.stream.StreamingFileSink.BulkEncodedSink

import com.yyb.flink10.sink.ParquetWriterSink
import org.apache.avro.reflect.ReflectData
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.ParquetWriterFactory
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-18
  * @Time 17:40
  */
object WordCountFileSourceStreamFileSinkOfParquet {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)

    val fileOutputCofig = OutputFileConfig
      .builder()
      .withPartSuffix(".parquet")
      .build()

    val fileSourcePath = "./data/data.txt"
    val fileSinkPath = "./xxx.text/rs2"
    val fileSinkPath1 = "./xxx.text/rs3"
    val fileSinkPath2 = "./xxx.text/rs4"

    val wc = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map(WC(_, 1))
      .keyBy(0)
      .sum(1)


//    val parquetSink = StreamingFileSink.forBulkFormat(new Path(fileSinkPath),
//      ParquetAvroWriters.forReflectRecord(classOf[WC]))
////      .withNewBucketAssigner(new BasePathBucketAssigner())
////      .withOutputFileConfig(fileOutputCofig)
////      .withRollingPolicy(OnCheckpointRollingPolicy.build())
//      .withBucketCheckInterval(10)
//      .build()


//    val txtSink: StreamingFileSink[WC] = StreamingFileSink.forRowFormat(new Path(fileSinkPath1),
//      new SimpleStringEncoder[WC]()
//    ).build()

    val parquetSinkmy = new ParquetWriterSink[WC](fileSinkPath2,
      ReflectData.get.getSchema(classOf[WC]).toString,
      CompressionCodecName.UNCOMPRESSED)

    wc.addSink(parquetSinkmy)

//    wc.addSink(txtSink).setParallelism(1)

    env.execute("WordCountFileSourceStreamFileSinkOfParquet")
  }

  case class WC(word:String, ct:Int)
}
