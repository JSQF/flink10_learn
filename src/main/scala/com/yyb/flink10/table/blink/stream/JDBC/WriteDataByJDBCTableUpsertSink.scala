package com.yyb.flink10.table.blink.stream.JDBC

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{BigIntType, DateType, IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-30
  * @Time 10:25
  */
object WriteDataByJDBCTableUpsertSink {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkStreamTable = StreamTableEnvironment.create(env, settings)

    val fileSourcePath = "./data/data.txt"

    val wcStream: DataStream[WordCount] = env.readTextFile(fileSourcePath)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .map(x => WordCount(x._1, x._2))


    val table: Table = blinkStreamTable.fromDataStream(wcStream)


    blinkStreamTable.createTemporaryView("wd", table)

    var sql =
      """
        |select * from wd
      """.stripMargin
    blinkStreamTable.sqlQuery(sql).printSchema()
    sql =
      s"""
         |select word , count(`count`) from wd group by word
         |""".stripMargin





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

    val tableSchema = TableSchema.builder()
      .field("word", new AtomicDataType(new VarCharType(2147483647))) //注意 String 就是 2147483647
      .field("count", new AtomicDataType(new BigIntType()))
      .build()

//    blinkStreamTable.registerTableSink("word1", Array("word", "count"), Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), jdbcUpsertTableSink)
    blinkStreamTable.registerTableSink("word1", jdbcAppendTableSink)


    //注意这里， 这里的 sql 需要的是 有 聚合操作的，如果没有的话， 那么 结果表里面就会出现多条记录，因为  jdbcUpsertTableSink 里面的 IsAppendOnly，KeyFields 是 flink 执行计划推断出来的
    blinkStreamTable.sqlQuery(sql).insertInto("word1")


    blinkStreamTable.execute("WriteDataByJDBCTableUpsertSink")

  }

  case class WordCount(word:String, count:Int)
}
