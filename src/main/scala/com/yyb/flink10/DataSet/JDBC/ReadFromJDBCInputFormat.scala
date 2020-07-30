package com.yyb.flink10.DataSet.JDBC

import java.sql.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.types.Row

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-26
  * @Time 09:52
  */
object ReadFromJDBCInputFormat {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val types =  Array[TypeInformation[_]](Types.INT, Types.STRING, Types.SQL_DATE)
    val fields = Array[String]("id", "name", "time")
    val typeInfo = new RowTypeInfo(types, fields)

    val jdbcInformat: JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setAutoCommit(false)
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDrivername("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setQuery("select * from t_order")
      .setRowTypeInfo(typeInfo)
      .finish()

    val AUX_TABLE: DataSet[Row] =  env.createInput(jdbcInformat)

    AUX_TABLE.print()

    val orderDataSet: DataSet[Order] =  AUX_TABLE.map(x => Order(x.getField(0).asInstanceOf[Int], x.getField(1).asInstanceOf[String], x.getField(2).asInstanceOf[Date]))
    orderDataSet.print()

//    env.execute("ReadFromJDBCInputFormat")

  }

  case class Order(id:Int, name:String, time:Date)
}
