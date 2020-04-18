package com.yyb.flink10.table.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-18
  * @Time 21:05
  */
object BatchQuery {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(env)

    val words = "hello flink hello lagou"
    val WDS = words.split("\\W+").map(WD(_, 1))

    val input: DataSet[WD] = env.fromCollection(WDS)

    val table: Table = batchTableEnv.fromDataSet(input)

    batchTableEnv.createTemporaryView("wordcount", table)

    batchTableEnv.sqlQuery("select * from wordcount").printSchema()


  }

  case class WD(word:String, count:Int)
}
