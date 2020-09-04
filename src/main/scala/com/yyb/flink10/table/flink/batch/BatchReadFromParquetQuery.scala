package com.yyb.flink10.table.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.formats.parquet.ParquetTableSource
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.parquet.schema.Type.Repetition

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-23
  * @Time 17:40
  */
object BatchReadFromParquetQuery {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(env)




    val word = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "word")
    val count = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "count")
    val schema = new MessageType("word", word, count)

    val parquetFile = new ParquetTableSource.Builder()
      .path("./xxx.text/rs2/2020-04-23--21/.part-0-0.parquet.inprogress.a6a2a7cd-98bf-4397-8e7d-558b1bb932aa")
      .forParquetSchema(schema)
        .build()

    batchTableEnv.registerTableSource("xx", parquetFile)

    val sql =
      """
        |select * from xx
      """.stripMargin
    val q1: Table =  batchTableEnv.sqlQuery(sql)

    q1.printSchema()

    batchTableEnv.toDataSet[WC](q1).print()
  }

  case class WC(word:String, ct:Int)
}
