package com.yyb.flink10.stream.sink.StreamingFileSink.RowEncodedSink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-18
  * @Time 17:17
  */
object WordCountFileSourceStreamFileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)


    val fileSourcePath = "/Users/yyb/Downloads/1.txt"
    val fileSinkPath = "./xxx.text/rs1"

    val wc: DataStream[(String, Int)] = env.readTextFile(fileSourcePath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val outputFileConfig = OutputFileConfig
      .builder()
      .withPartPrefix("filesource")
      .withPartSuffix(".finksink")
      .build()

    val fileSink: StreamingFileSink[(String, Int)] = StreamingFileSink.forRowFormat(new Path(fileSinkPath),
      new SimpleStringEncoder[(String, Int)]("UTF-8"))
      .withOutputFileConfig(outputFileConfig)
      .build()

//    wc.addSink(fileSink)
    wc.addSink(fileSink).setParallelism(1) //这样减少输出文件的个数，但是生产环境不建议使用，会影响性能

    env.execute("WordCountFileSourceStreamFileSink")


  }
}
