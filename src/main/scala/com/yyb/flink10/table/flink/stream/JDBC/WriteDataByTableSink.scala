package com.yyb.flink10.table.flink.stream.JDBC

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.connector.jdbc.internal.options.{JdbcLookupOptions, JdbcOptions, JdbcReadOptions}
import org.apache.flink.connector.jdbc.table.{JdbcTableSource, JdbcUpsertTableSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableSchema}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-30
  * @Time 09:16
  */
object WriteDataByTableSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(env)

    val fileSourcePath = "./data/data.txt"

    val wcStream: DataStream[(String, Int)] = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val table: Table =  streamTableEnv.fromDataStream(wcStream)

    streamTableEnv.createTemporaryView("wd", table)



    streamTableEnv.sqlQuery("select * from wd").printSchema()

    val jdbcOptions = JdbcOptions.builder()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDriverName("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setTableName("wordcount")
      .build()

    val schema: TableSchema = streamTableEnv.sqlQuery("select * from wd").getSchema

    val jdbcReadOptions = JdbcReadOptions.builder()
      .setFetchSize(2000)
      .setQuery("insert into wordcount (word, count) values(?, ?)")
      .build();
    val jdbcLookupOptions = JdbcLookupOptions.builder()
      .setCacheExpireMs(0)
      .setCacheMaxSize(0)
      .setMaxRetryTimes(3000)
      .build();

    val jdbcAppendTableSink = JdbcUpsertTableSink.builder()
        .setOptions(jdbcOptions)
        .build()

//    streamTableEnv.registerTableSink("mysql_wordcount", Array("word", "count").asInstanceOf[String], Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO).asInstanceOf[String], jdbcAppendTableSink)
    streamTableEnv.registerTableSink("mysql_wordcount", jdbcAppendTableSink)
    table.insertInto("mysql_wordcount")

    streamTableEnv.execute("WriteDataByTableSink")
  }
}
