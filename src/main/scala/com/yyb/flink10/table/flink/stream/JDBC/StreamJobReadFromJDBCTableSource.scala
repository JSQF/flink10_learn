package com.yyb.flink10.table.flink.stream.JDBC

import org.apache.flink.api.java.io.jdbc.{JDBCLookupOptions, JDBCOptions, JDBCReadOptions, JDBCTableSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableSchema}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{DateType, IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-26
  * @Time 21:39
  */
object StreamJobReadFromJDBCTableSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(env)

    val lookOption =  JDBCLookupOptions.builder()
      .setCacheExpireMs(60*1000)
      .setCacheMaxSize(1024*1024)
      .setMaxRetryTimes(10)
      .build()

    val jdbcOpition = JDBCOptions.builder()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDriverName("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setTableName("t_order")
      .build()

    val jdbcReadOption = JDBCReadOptions.builder()
      .setFetchSize(5000)
      .build()

    val tableSchema = TableSchema.builder()
      .field("id", new AtomicDataType(new IntType))
      .field("name", new AtomicDataType(new VarCharType(2147483647))) //注意 String 就是 2147483647
      .field("time", new AtomicDataType(new DateType))
      .build()

    val jdbcTableSource =  JDBCTableSource.builder()
      .setLookupOptions(lookOption)
      .setOptions(jdbcOpition)
      .setReadOptions(jdbcReadOption)
      .setSchema(tableSchema)
      .build()

    val t_order: Table =  streamTableEnv.fromTableSource(jdbcTableSource)

    streamTableEnv.registerTableSource("t_order1", jdbcTableSource)

    streamTableEnv.createTemporaryView("t_order", t_order)

    val sql =
      """
        |select * from t_order
      """.stripMargin
    streamTableEnv.sqlQuery(sql).printSchema()

//    streamTableEnv.registerTableSink()


  }
}
