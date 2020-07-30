package com.yyb.flink10.DataSet.JDBC

import com.yyb.flink10.DataStream.data.WordCountData
import com.yyb.flink10.DataStream.parquet.WriteParquetWordCount.WORDCOUNT
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-26
  * @Time 10:32
  */
object WriteToMysqlByOutputformat {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements(WordCountData.WORDS: _*)
    val counts: DataSet[WORDCOUNT] = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map(WORDCOUNT(_, 1))
      .groupBy(0)
      .sum(1)
    val countRecord: DataSet[Row] = counts.map(x =>  Row.of(x.word, x.count.asInstanceOf[Integer]))

    val mysqlOutput: JDBCOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
      .setDrivername("com.mysql.jdbc.Driver")
      .setUsername("root")
      .setPassword("111111")
      .setQuery("insert into  wordcount (word, count) values(?, ?)") //注意这里是 mysql 的插入语句
      .setSqlTypes(Array(java.sql.Types.VARCHAR, java.sql.Types.INTEGER)) //这里是每行数据的 类型
      .finish()

    countRecord.output(mysqlOutput)


    env.execute("WriteToMysqlByOutputformat")
  }
}
