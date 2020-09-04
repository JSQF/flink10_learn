package com.yyb.flink10.DataStream.sink.JDBC

import com.yyb.flink10.DataStream.data.WordCountData
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.types.Row

/**
  * @Author yyb
  * @Description 注意在 DataStream 的模式下 会出现 多条聚合数据
  * @Date Create in 2020-04-26
  * @Time 13:50
  */
object WriteToMysqlByJDBCOutputformat {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.fromElements(WordCountData.WORDS: _*)
    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val mysqlOutput: JDBCOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDrivername("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setQuery("insert into  wordcount (word, count) values(?, ?)") //注意这里是 mysql 的插入语句
      .setSqlTypes(Array(java.sql.Types.VARCHAR, java.sql.Types.INTEGER)) //这里是每行数据的 类型
      .finish()

//    val jdbcSink = new JDBCSinkFunction(mysqlOutput) //注意这个 类不能这样实用化，因为它不是 public class


    val countRecord: DataStream[Row] = counts.map(x =>  Row.of(x._1, x._2.asInstanceOf[Integer]))


    countRecord.writeUsingOutputFormat(mysqlOutput)


    env.execute("WriteToMysqlByJDBCOutputformat")

  }

}
