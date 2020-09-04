package com.yyb.flink10.DataStream.kafka

import java.io.InputStream
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
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
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val in_env: InputStream = ClassLoader.getSystemResourceAsStream("env.properties")
    val prop: Properties = new Properties()
    prop.load(in_env)

    val data = Array(Current(1, "Euro"))

    val dataDS: DataStream[Current] = env.fromCollection(data)


    val kafkaSink = new FlinkKafkaProducer011[String](
      prop.getProperty("bootstrap.servers"),            // broker list
      "eventsource_yyb",                  // target topic
      new SimpleStringSchema());   // serialization schema
    kafkaSink.setWriteTimestampToKafka(true)

    dataDS.map(_.toString).addSink(kafkaSink)


    env.execute("SendData2KafkaByKafkaConnector")
  }

  case class Current(amount:Int, currency:String)
}
