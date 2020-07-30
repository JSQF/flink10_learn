package com.yyb.flink10.DataStream.sink.StreamingFileSink.BulkEncodedSink

import com.yyb.flink10.DataStream.sink.StreamingFileSink.BulkEncodedSink.WordCountFileSourceStreamFileSinkOfParquet.WC
import org.apache.flink.api.java.tuple
import org.apache.hadoop.conf.Configuration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory
import org.apache.flink.runtime.util.HadoopUtils
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-18
  * @Time 18:25
  */
object WordCountFileSourceStreamFileSinkOfSequence {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.generateSequence(1, 100)

    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)

    val fileOutputCofig = OutputFileConfig
      .builder()
      .withPartSuffix(".sequence")
      .build()

    val fileSinkPath = "./xxx.text/rs3"

    val wc: DataStream[tuple.Tuple2[LongWritable, Text]] = env.generateSequence(1, 100).map(x => new tuple.Tuple2(new LongWritable(x), new Text(x.toString)))




    /**
      * 这里的 LongWritable 和 Text 都是 org.apache.hadoop.io 下的包，
      * 是在 hadoop-common 依赖中的，你可以直接 依赖这个包，
      * 也可以 依赖 hadoop-client 这个包， hadoop-client 这个包里面 有 hadoop-common 这个依赖。
      */
    val hadoopConf: Configuration = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration())
    val parquetSink: StreamingFileSink[tuple.Tuple2[LongWritable, Text]] = StreamingFileSink.forBulkFormat(new Path(fileSinkPath),
      new SequenceFileWriterFactory(hadoopConf, classOf[LongWritable], classOf[Text]))
      //      .withNewBucketAssigner(new BasePathBucketAssigner())
      .withOutputFileConfig(fileOutputCofig)
      .build()

    wc.addSink(parquetSink).setParallelism(1)

    env.execute("WordCountFileSourceStreamFileSinkOfParquet")
  }
}
