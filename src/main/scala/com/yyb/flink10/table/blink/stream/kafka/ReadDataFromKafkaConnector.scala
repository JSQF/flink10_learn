package com.yyb.flink10.table.blink.stream.kafka

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.descriptors.{ConnectTableDescriptor, Json, Kafka, Schema}

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-06-10
  * @Time 09:32
  */
object ReadDataFromKafkaConnector {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val flinkTableEnv = StreamTableEnvironment.create(env, settings)

    val kafka = new Kafka()
    kafka.version("0.11")
      .topic("eventsource_yhj")
      .property("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181")
      .property("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092")
      .property("group.id", "yyb_dev")
      .startFromEarliest()

    val schema = new Schema()
    val tableSchema =  TableSchema.builder()
        .field("id", DataTypes.STRING())
        .field("time", DataTypes.STRING())
        .build()
    schema.schema(tableSchema)
    val tableSource: ConnectTableDescriptor =  flinkTableEnv.connect(kafka)
      .withFormat( new Json().failOnMissingField(true) )
      .withSchema(schema)
    tableSource.createTemporaryTable("test")
    var sql = "select * from test"

    val test: Table =  flinkTableEnv.from("test")
    test.printSchema()


    val testDataStream: DataStream[Pi] =  flinkTableEnv.toAppendStream[Pi](test)

    testDataStream.print().setParallelism(1)

    flinkTableEnv.execute("ReadDataFromKafkaConnector")


  }

  case class Pi(
               id:String,
               time:String
               )
}
