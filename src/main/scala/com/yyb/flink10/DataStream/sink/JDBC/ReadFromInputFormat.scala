package com.yyb.flink10.DataStream.sink.JDBC

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-29
  * @Time 16:25
  */
object ReadFromInputFormat {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val types =  Array[TypeInformation[_]](Types.INT, Types.STRING, Types.SQL_DATE)
    val fields = Array[String]("id", "name", "time")
    val typeInfo = new RowTypeInfo(types, fields)
    val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDrivername("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setQuery("select * from t_order")
      .setRowTypeInfo(typeInfo)
      .finish()

    val t_order: DataStream[Row] =  env.createInput(jdbcInputFormat)
    t_order.print()

    env.execute("ReadFromInputFormat")

//    t_order.addSink() //flink-jdbc 的 sinkFunction 都是 非 public的，不可用的，里面的 sinkFunction 是在 tableSource 中使用的
  }
}
