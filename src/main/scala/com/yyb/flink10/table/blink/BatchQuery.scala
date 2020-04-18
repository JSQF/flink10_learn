package com.yyb.flink10.table.blink

import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/**
  * @Author yyb
  * @Description 注意 Blink
  * @Date Create in 2020-04-18
  * @Time 21:05
  */
object BatchQuery {
  def main(args: Array[String]): Unit = {
   val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
   val bbTableEnv = TableEnvironment.create(bbSettings)




    val input: Table = bbTableEnv.from("")

    bbTableEnv.createTemporaryView("wordcount", input)

    bbTableEnv.sqlQuery("select * from wordcount").printSchema()




  }

  case class WORDCOUNT(word:String, count:Int)
}
