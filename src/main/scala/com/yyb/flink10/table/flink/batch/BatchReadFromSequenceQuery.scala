package com.yyb.flink10.table.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.datanucleus.store.rdbms.valuegenerator.SequenceTable

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-24
  * @Time 09:29
  */
object BatchReadFromSequenceQuery {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(env)

//    env.readFileOfPrimitives()
  }
}
