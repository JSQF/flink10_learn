package com.yyb.flink10.table.blink.stream.JDBC

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-30
  * @Time 10:25
  */
object WriteDataByJDBCTableSink {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkStreamTable = StreamTableEnvironment.create(env, settings)

    val fileSourcePath = "./data/data.txt"

    val wcStream: DataStream[(String, Int)] = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)


    val table: Table = blinkStreamTable.fromDataStream(wcStream)

    blinkStreamTable.createTemporaryView("wd", table)

    var sql =
      """
        |select * from wd
      """.stripMargin

    blinkStreamTable.sqlQuery(sql).printSchema()

    val jdbcAppendTableSink =  JDBCAppendTableSink.builder()
      .setBatchSize(5000)
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDrivername("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setQuery("insert into  wordcount (word, count) values(?, ?)")
      .setParameterTypes(java.sql.Types.VARCHAR, java.sql.Types.INTEGER)
      .build()

    blinkStreamTable.registerTableSink("word1", Array("word", "count"), Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), jdbcAppendTableSink)

    table.insertInto("word1")

    blinkStreamTable.execute("WriteDataByJDBCTableSink")

  }
}
