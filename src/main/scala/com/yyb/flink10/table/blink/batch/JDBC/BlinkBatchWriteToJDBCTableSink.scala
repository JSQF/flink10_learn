package com.yyb.flink10.table.blink.batch.JDBC


import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{IntType, VarCharType}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-27
  * @Time 13:19
  */
object BlinkBatchWriteToJDBCTableSink {
  def main(args: Array[String]): Unit = {
    //blink env
    val settings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build()
    val blinkBatchTableEnv = TableEnvironment.create(settings)

    val sourceFIlePath = "/Users/yyb/Downloads/1_rs.csv"

    val stringField = new AtomicDataType(new VarCharType(50))
    val intField = new AtomicDataType(new IntType)
    val csvTableSource: CsvTableSource =  CsvTableSource.builder()
      .path(sourceFIlePath)
      .field("word", stringField)
      //      .field("word", Types.STRING) //方法已压制
      .field("count", intField)
      //      .field("count", Types.INT) //方法已压制
      .build()

    val word: Table =  blinkBatchTableEnv.fromTableSource(csvTableSource)

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


    blinkBatchTableEnv.createTemporaryView("word", word)

    val sql =
      s"""
         |select * from word
       """.stripMargin
    blinkBatchTableEnv.sqlQuery(sql).printSchema()

    blinkBatchTableEnv.registerTableSink("word1", Array("word", "count"), Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), jdbcAppendTableSink)

    blinkBatchTableEnv.sqlQuery(sql).insertInto("word1") //注意 这样 把数据 倒入到 sink 中去


    blinkBatchTableEnv.execute("BlinkBatchWriteToJDBCTableSink")

  }
}
