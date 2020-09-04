package com.yyb.flink10.table.blink.stream.JDBC

import org.apache.flink.connector.jdbc.internal.options.{JdbcLookupOptions, JdbcOptions, JdbcReadOptions}
import org.apache.flink.connector.jdbc.table.JdbcTableSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{DateType, IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-30
  * @Time 10:01
  */
object ReadDataFromJDBCTableSource {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkStreamTable = StreamTableEnvironment.create(env, settings)

    val fileSourcePath = "/Users/yyb/Downloads/1.txt"

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

    val lookOption =  JdbcLookupOptions.builder()
      .setCacheExpireMs(60*1000)
      .setCacheMaxSize(1024*1024)
      .setMaxRetryTimes(10)
      .build()

    val jdbcOpition = JdbcOptions.builder()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDriverName("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setTableName("t_order")
      .build()

    val jdbcReadOption = JdbcReadOptions.builder()
      .setFetchSize(5000)
      .build()

    val tableSchema = TableSchema.builder()
      .field("id", new AtomicDataType(new IntType))
      .field("name", new AtomicDataType(new VarCharType(2147483647))) //注意 String 就是 2147483647
      .field("time", new AtomicDataType(new DateType))
      .build()

    val jdbcTableSource: JdbcTableSource =  JdbcTableSource.builder()
      .setLookupOptions(lookOption)
      .setOptions(jdbcOpition)
      .setReadOptions(jdbcReadOption)
      .setSchema(tableSchema)
      .build()

    blinkStreamTable.registerTableSource("mysql_t_order", jdbcTableSource)

    blinkStreamTable.sqlQuery("select * from mysql_t_order")


  }
}
