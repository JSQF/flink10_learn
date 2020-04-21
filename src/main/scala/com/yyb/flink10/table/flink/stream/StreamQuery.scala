package com.yyb.flink10.table.flink.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-19
  * @Time 12:44
  */
object StreamQuery {
  def main(args: Array[String]): Unit = {
    //注意 这里新加了一个 EnvironmentSettings
    val flinkStreamSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(env, flinkStreamSettings)

    val fileSourcePath = "/Users/yyb/Downloads/1.txt"

    val wcStream: DataStream[(String, Int)] = env.readTextFile(fileSourcePath)
        .flatMap(_.split("\\W+"))
        .filter(_.nonEmpty)
        .map((_, 1))
        .keyBy(0)
        .sum(1)

    val table: Table =  streamTableEnv.fromDataStream(wcStream)

    streamTableEnv.createTemporaryView("wd", table)



    streamTableEnv.sqlQuery("select * from wd").printSchema()

    val appendDateStream: DataStream[WD] =  streamTableEnv.toAppendStream[WD](table)

    appendDateStream.print()


    env.execute("StreamQuery")
  }

  case class WD(word:String, count:Int)
}
