package com.yyb.flink10.table.flink.batch

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.formats.parquet.ParquetTableSource
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{BatchTableDescriptor, FileSystem}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.types.Row

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-21
  * @Time 14:15
  */
object ConnectJDBCBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(env)

    val types =  Array[TypeInformation[_]](Types.STRING, Types.LONG, Types.STRING)
    val fields = Array[String]("MT_KEY1", "MT_KEY2", "MT_COMMENT")
    val typeInfo = new RowTypeInfo(types, fields)


    val jdbc = new JDBCInputFormatBuilder()
        .setDBUrl("jdbc:mysql://127.0.0.1:3306/hive?useSSL=false&serverTimezone=UTC")
        .setDrivername("com.mysql.jdbc.Driver")
        .setUsername("hive")
        .setPassword("hive")
        .setQuery("select * from AUX_TABLE")
      .setRowTypeInfo(typeInfo)
        .finish()
    val mysqlSource : DataSet[Row] =  env.createInput(jdbc)

    mysqlSource.print()

//    val table: ParquetTableSource = new ParquetTableSource()
//    batchTableEnv.registerTableSource("table", table)

    val file = new FileSystem() //注意这里只是 实验性质
    val fs: BatchTableDescriptor =  batchTableEnv.connect(file)

//    val jdbcTableSink = new JDBCAppendTableSink()
//    batchTableEnv.registerTableSink("jdbcTableSink", jdbcTableSink)


    val csvTableSink = new CsvTableSink("")
    batchTableEnv.registerTableSink("csvTableSink", csvTableSink)



    //目前来看，只有在 有 sink的情况下，需要 加 execute
//    batchTableEnv.execute("ConnectJDBCBatch")
  }
}
