package com.yyb.flink10.table.flink.batch.JDBC

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.connector.jdbc.internal.options.{JdbcLookupOptions, JdbcOptions, JdbcReadOptions}
import org.apache.flink.connector.jdbc.table.JdbcTableSource
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableSchema}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{DateType, IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-26
  * @Time 17:33
  */
object BatchJobReadFromJDBCTableSource {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(env)

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

    val t_order: Table = batchTableEnv.fromTableSource(jdbcTableSource)

    batchTableEnv.createTemporaryView("t_order", t_order)

    val sql =
      s"""
         |select * from t_order
       """.stripMargin
    batchTableEnv.sqlQuery(sql).printSchema()

  }
}
