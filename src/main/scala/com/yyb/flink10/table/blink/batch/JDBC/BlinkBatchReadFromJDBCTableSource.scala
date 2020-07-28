package com.yyb.flink10.table.blink.batch.JDBC

import org.apache.flink.api.java.io.jdbc.{JDBCLookupOptions, JDBCOptions, JDBCReadOptions, JDBCTableSource}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment, TableSchema}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{DateType, IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-27
  * @Time 11:09
  */
object BlinkBatchReadFromJDBCTableSource {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val blinkBatchTableEnv = TableEnvironment.create(settings)

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

    val jdbcTableSource: JDBCTableSource =  JDBCTableSource.builder()
      .setLookupOptions(lookOption)
      .setOptions(jdbcOpition)
      .setReadOptions(jdbcReadOption)
      .setSchema(tableSchema)
      .build()


    val t_order: Table = blinkBatchTableEnv.fromTableSource(jdbcTableSource)

    blinkBatchTableEnv.createTemporaryView("t_order", t_order)

    blinkBatchTableEnv.sqlQuery("select * from t_order").printSchema()



  }
}
