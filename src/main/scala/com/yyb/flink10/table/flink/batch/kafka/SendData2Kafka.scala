package com.yyb.flink10.table.flink.batch.kafka

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.descriptors.{ConnectTableDescriptor, Json, Kafka, Schema}


/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-07-28
  * @Time 16:12
  */
object SendData2Kafka {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkTableEnv = StreamTableEnvironment.create(env, settings)

    val kafka = new Kafka
    kafka.version("0.11")
      .topic("eventsource_yhj")
      .property("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181")
      .property("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092").
      property("group.id", "yyb_dev")
      .startFromLatest


    val schema = new Schema
    val tableSchema1 = TableSchema.builder
      .field("amount", DataTypes.INT)
      .field("currency", DataTypes.STRING).build
    schema.schema(tableSchema1)
    val tableSource = blinkTableEnv.connect(kafka)
      .withFormat(new Json().failOnMissingField(true))
      .withSchema(schema)
    tableSource.createTemporaryTable("Orders_tmp")

    val data = Array(Current(1, "Euro"))

    val dataDS = env.fromCollection(data)

    val dataTable: Table = blinkTableEnv.fromDataStream(dataDS)

//    blinkTableEnv.registerTable("dataSource", dataTable)

    var sql =
      """
        |insert into Orders_tmp select * from dataSource
        |""".stripMargin
//    blinkTableEnv.sqlUpdate(sql)

    dataTable.insertInto("Orders_tmp")


    env.execute("SendData2Kafka")
  }

  case class Current(amount:Int, currency:String)
}
