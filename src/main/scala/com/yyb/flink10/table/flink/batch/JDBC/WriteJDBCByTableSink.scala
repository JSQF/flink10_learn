package com.yyb.flink10.table.flink.batch.JDBC

import com.yyb.flink10.table.flink.batch.BatchQuery.WD
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-29
  * @Time 17:25
  */
object WriteJDBCByTableSink {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(env)

    val words = "hello flink hello lagou"
    val WDS = words.split("\\W+").map(WD(_, 1))

    val input: DataSet[WD] = env.fromCollection(WDS)

    val table: Table = batchTableEnv.fromDataSet(input)


    batchTableEnv.createTemporaryView("wordcount", table)


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


    batchTableEnv.registerTableSink("wordcount_jdbc",  Array("word", "count"), Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), jdbcAppendTableSink)

    table.insertInto("wordcount_jdbc")

    batchTableEnv.execute("WriteJDBCByTableSink")

  }
}
