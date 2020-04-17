package com.yyb.flink10.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-15
  * @Time 17:00
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePtah = "/Users/yyb/Downloads/1.txt"
    val filepathtosave = "/Users/yyb/Downloads/1_rs.csv"
    val text =  env.readTextFile(filePtah)
    val wordCounts = text.flatMap(_.toLowerCase.split("\\W+") filter { _.nonEmpty})
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCounts.setParallelism(1).print()
    wordCounts.setParallelism(1).writeAsCsv(filepathtosave)


    env.execute("WordCount")
  }
}
