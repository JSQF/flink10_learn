package com.yyb.flink10.stream.parquet

import java.util.concurrent.TimeUnit

import com.yyb.flink10.stream.WordCountData
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-16
  * @Time 09:53
  */
object WriteParquetWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)



//    env.enableCheckpointing(1000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
//    // checkpoint错误次数，是否任务会失败
//    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
//    env.setStateBackend(new FsStateBackend("/tmp/xxx").asInstanceOf[StateBackend])




    val config = OutputFileConfig
      .builder()
      .withPartPrefix("wordcount")
      .withPartSuffix(".exe")
      .build()

    val text =
      if(params.has("--input")){
        env.readTextFile(params.get("--input"))
      }else{
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WordCountData.WORDS: _*)
      }

    val counts: DataStream[WORDCOUNT] = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map(WORDCOUNT(_, 1))
      .keyBy(0)
      .sum(1)

    if(params.has("--output")){
      counts.writeAsText(params.get("--output"))
      //注意这里的 范型 要和 counts 的范型 一致
      val filesink: StreamingFileSink[WORDCOUNT] = StreamingFileSink
        .forBulkFormat(new Path(params.get("--output")), ParquetAvroWriters.forReflectRecord(classOf[WORDCOUNT]))
        .withBucketAssigner(new DateTimeBucketAssigner())
//        .withRollingPolicy(OnCheckpointRollingPolicy.build())
//        .withOutputFileConfig(config) // 设置输出文件的 前后缀
        .build()

      val sink: StreamingFileSink[WORDCOUNT] = StreamingFileSink

        .forRowFormat(new Path(""), new SimpleStringEncoder[WORDCOUNT]("UTF-8"))
//        .withBucketAssigner(new DateTimeBucketAssigner())
//        .withRollingPolicy(OnCheckpointRollingPolicy.build())

//        .withOutputFileConfig(config)
        .build()



      counts.addSink(filesink)

    }else{
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    env.execute("StreamWordCount")
  }

  case class WORDCOUNT(word:String, count:Int)
}
