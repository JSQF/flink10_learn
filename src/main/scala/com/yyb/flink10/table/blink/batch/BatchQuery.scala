package com.yyb.flink10.table.blink.batch

import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{IntType, VarCharType}

/**
  * @Author yyb
  * @Description 注意 Blink
  * @Date Create in 2020-04-18
  * @Time 21:05
  */
object BatchQuery {
  def main(args: Array[String]): Unit = {
   val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
   val bbTableEnv: TableEnvironment = TableEnvironment.create(bbSettings)



    val sourceFIlePath = "/Users/yyb/Dwonloads/1_rs.csv"

    val stringField = new AtomicDataType(new VarCharType(50))
    val intField = new AtomicDataType(new IntType)
    val csvTableSource =  CsvTableSource.builder()
      .path(sourceFIlePath)
      .field("word", stringField)
//      .field("word", Types.STRING) //方法已压制
      .field("count", intField)
//      .field("count", Types.INT) //方法已压制
      .build()

    val input: Table = bbTableEnv.fromTableSource(csvTableSource)




    bbTableEnv.createTemporaryView("wordcount", input)

    bbTableEnv.sqlQuery("select * from wordcount").printSchema()



    bbTableEnv.execute("BatchQuery")


  }

  case class WORDCOUNT(word:String, count:Int)
}
