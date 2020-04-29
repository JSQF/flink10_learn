package com.yyb.flink10.table.flink.stream.JDBC

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-21
  * @Time 14:15
  */
object StreamJDBCReadByInputformat2TableSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(env)

    val types =  Array[TypeInformation[_]](Types.STRING, Types.LONG, Types.STRING)
    val fields = Array[String]("MT_KEY1", "MT_KEY2", "MT_COMMENT")
    val typeInfo = new RowTypeInfo(types, fields)


    val jdbc = new JDBCInputFormatBuilder()
        .setDBUrl("jdbc:mysql://127.0.0.1:3306/hive?useSSL=false&serverTimezone=UTC")
        .setDrivername("com.mysql.jdbc.Driver")
        .setUsername("hive")
        .setPassword("hive")
        .setQuery("select * from AUX_TABLE")
      .setRowTypeInfo(typeInfo)
        .finish()
    val mysqlSource: DataStream[Row] =  env.createInput(jdbc)

    mysqlSource.print()

    val table: Table = streamTableEnv.fromDataStream(mysqlSource)

    streamTableEnv.createTemporaryView("AUX_TABLE", table)

    val table_q: Table = streamTableEnv.sqlQuery("select * from AUX_TABLE")
    table_q.printSchema()






    //目前来看，只有在 有 sink的情况下，需要 加 execute
    streamTableEnv.execute("ConnectJDBCBatch")
  }
}
