package com.yyb.flink10.table.blink.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-18
  * @Time 21:05
  */
object StreamQuery {
  def main(args: Array[String]): Unit = {
    val blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, blinkStreamSettings)

    val fileSourcePath = "/Users/yyb/Downloads/1.txt"

    val wcStream: DataStream[(String, Int)] = streamExecutionEnvironment.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)


    val table: Table = streamTableEnv.fromDataStream(wcStream)

    streamTableEnv.createTemporaryView("wd", table)

    var sql =
      """
        |select * from wd
      """.stripMargin

    streamTableEnv.sqlQuery(sql).printSchema()

    val dataStream: DataStream[WD] =  streamTableEnv.toAppendStream[WD](table)

    dataStream.print()

    streamTableEnv.execute("StreamQuery")
  }

  case class WD(word:String, count:Int)
}
