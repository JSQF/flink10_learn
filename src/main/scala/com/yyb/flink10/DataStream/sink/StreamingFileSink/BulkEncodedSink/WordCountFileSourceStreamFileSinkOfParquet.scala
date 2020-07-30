package com.yyb.flink10.DataStream.sink.StreamingFileSink.BulkEncodedSink

import com.yyb.flink10.sink.ParquetWriterSink
import org.apache.avro.reflect.ReflectData
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.ParquetWriterFactory
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
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
    System.setProperty("HADOOP_USER_NAME", "yyb")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)

    env.enableCheckpointing(20)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val fileOutputCofig = OutputFileConfig
      .builder()
      .withPartSuffix(".parquet")
      .build()

    val fileSourcePath = "./data/data.txt"
    val fileSinkPath = "./xxx.text/rs2"
    val fileSinkPath1 = "./xxx.text/rs3"
    val fileSinkPath2 = "file:///Users/yyb/ScalaSource/flink10_learn/xxx.text/rs4"

    val wc = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map(WC(_, 1))
      .keyBy(0)
      .sum(1)

      val simleSink = StreamingFileSink.forRowFormat(new Path(fileSinkPath), new SimpleStringEncoder[WC]())
      .build()

    // 注意在使用这种方式的 parquet sink 与 一次运行的 dataStream 配合的时候，一般会出现 parquet文件没有写完整的问题
    val parquetSink: StreamingFileSink[WC] = StreamingFileSink.forBulkFormat(new Path(fileSinkPath),
      ParquetAvroWriters.forReflectRecord(classOf[WC]))
//      .withNewBucketAssigner(new BasePathBucketAssigner())
//      .withOutputFileConfig(fileOutputCofig)
//      .withBucketCheckInterval(10)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    wc.print()

//    wc.addSink(parquetSink).setParallelism(1)

    val parquetSinkmy = new ParquetWriterSink[WC](fileSinkPath2,
      ReflectData.get.getSchema(classOf[WC]).toString,
      CompressionCodecName.UNCOMPRESSED)

    wc.addSink(parquetSinkmy)

//    wc.addSink(txtSink).setParallelism(1)


    env.execute("WordCountFileSourceStreamFileSinkOfParquet")
  }

  case class WC(word:String, ct:Int)
}
