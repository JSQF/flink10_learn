package com.yyb.flink10.table.flink.stream.kafka

import java.io.InputStream
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}


/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-07-28
  * @Time 16:12
  */
object SendData2KafkaByKafkaConnector {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkTableEnv = StreamTableEnvironment.create(env, settings)

    val in_env: InputStream = ClassLoader.getSystemResourceAsStream("env.properties")
    val prop: Properties = new Properties()
    prop.load(in_env)
    println(prop.getProperty("zookeeper.connect"))

    val kafka = new Kafka
    kafka.version("0.11")
      .topic("eventsource_yhj")
      .property("zookeeper.connect", prop.getProperty("zookeeper.connect"))
      .property("bootstrap.servers", prop.getProperty("bootstrap.servers")).
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
    //因为 kafka 是 无界的， 所以不能使用 batch 模式 的 kafkatablesink
//    blinkTableEnv.sqlUpdate(sql)

    dataTable.insertInto("Orders_tmp")


    env.execute("SendData2KafkaByKafkaConnector")
  }

  case class Current(amount:Int, currency:String)
}
