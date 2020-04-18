package com.yyb.flink10.stream

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{BasePathBucketAssigner, DateTimeBucketAssigner}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-15
  * @Time 21:45
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)

    val config = OutputFileConfig
      .builder()
      .withPartPrefix("wordcount")
      .withPartSuffix("exe")
      .build()

    val text: DataStream[String] =
      if(params.has("--input")){
        env.readTextFile(params.get("--input"))
      }else{
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WordCountData.WORDS: _*)
      }

    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    if(params.has("--output")){
      counts.writeAsText(params.get("--output")) //方法已经被 压制，推荐使用下面的 sink 方法
      //注意这里的 范型 要和 counts 的范型 一致
      /**
        * 注意这里有一个 scala 的 多个 .with。。。 build的bug，可以使用 java 版本编写，或者提高 flink的版本
        */
      val filesink: StreamingFileSink[(String, Int)] = StreamingFileSink
        .forRowFormat(new Path(params.get("--output")), new SimpleStringEncoder[(String, Int)]("UTF-8"))
//        .withRollingPolicy(
//          DefaultRollingPolicy.builder()
//            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//            .withMaxPartSize(1024 * 1024 * 1024)
//            .build())
//         .withBucketAssigner(new DateTimeBucketAssigner()) //这种方式，即以时间格式 yyyy-MM-dd--HH，可以自己修改 以时间格式 和 时区

        .withBucketAssigner(new BasePathBucketAssigner[(String, Int)]) //这种方式就是 不会指定 子文件的命名方式。
//        .withOutputFileConfig(config) // 设置输出文件的 前后缀
        .build()

      counts.addSink(filesink)

    }else{
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    env.execute("StreamWordCount")
  }
}
