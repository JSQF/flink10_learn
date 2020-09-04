package com.yyb.flink10.table.flink.stream.JDBC

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableSchema}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-05-05
  * @Time 13:04
  */
object WriteDataByJDBCTableUpsertSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val flinkSteramTableEnv = StreamTableEnvironment.create(env)

    val fileSourcePath = "./data/data.txt"

    val wcStream: DataStream[(String, Int)] = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val source: Table =  flinkSteramTableEnv.fromDataStream(wcStream)

    flinkSteramTableEnv.registerTable("word_flink", source)

    val jdbcOptions = JdbcOptions.builder()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDriverName("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setTableName("wordcount")
      .build()
    val jdbcAppendTableSink = JdbcUpsertTableSink.builder()
      .setOptions(jdbcOptions)
      .build()
    flinkSteramTableEnv.registerTableSink("word_mysql", Array("word", "count"), Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), jdbcAppendTableSink)
    //注意这里， 这里的 sql 需要的是 有 聚合操作的，如果没有的话， 那么 结果表里面就会出现多条记录，因为  jdbcUpsertTableSink 里面的 IsAppendOnly，KeyFields 是 flink 执行计划推断出来的
    source.insertInto("word_mysql")

    flinkSteramTableEnv.execute("WriteDataByJDBCTableUpsertSink")
  }
}
