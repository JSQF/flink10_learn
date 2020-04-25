package com.yyb.flink10.stream.StreamingFileSink.BulkEncodedSink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

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

    env.enableCheckpointing(20)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val fileOutputCofig = OutputFileConfig
      .builder()
      .withPartSuffix(".parquet")
      .build()

    val fileSourcePath = "/Users/yyb/Downloads/1.txt"
    val fileSinkPath = "./xxx.text/rs2"

    val wc = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map(WC(_, 1))
      .keyBy(0)
      .sum(1)

      val simleSink = StreamingFileSink.forRowFormat(new Path(fileSinkPath), new SimpleStringEncoder[WC]())
      .build()

    val parquetSink = StreamingFileSink.forBulkFormat(new Path(fileSinkPath),
      ParquetAvroWriters.forReflectRecord(classOf[WC]))
//      .withNewBucketAssigner(new BasePathBucketAssigner())
//      .withOutputFileConfig(fileOutputCofig)
//      .withBucketCheckInterval(10)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    wc.print()

    wc.addSink(parquetSink).setParallelism(1)


    env.execute("WordCountFileSourceStreamFileSinkOfParquet")
  }

  case class WC(word:String, ct:Int)
}
