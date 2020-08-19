package com.yyb.flink10.DataStream.kafka

import java.io.InputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.yyb.flink10.commonEntity.{ProductInfo, UserBrowseLog}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-07-28
  * @Time 16:12
  */
object SendData2KafkaByKafkaConnectorProduct {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val in_env: InputStream = ClassLoader.getSystemResourceAsStream("env.properties")
    val prop: Properties = new Properties()
    prop.load(in_env)

    val data = Array(
      new ProductInfo("product_5", "name50", "category50", "2016-01-01 00:00:00", 0L),
      new ProductInfo("product_5", "name52", "category52", "2016-01-01 00:00:02", 0L),
      new ProductInfo("product_5", "name55", "category55", "2016-01-01 00:00:05", 0L),
      new ProductInfo("product_3", "name32", "category32", "2016-01-01 00:00:02", 0L),
      new ProductInfo("product_3", "name35", "category35", "2016-01-01 00:00:05", 0L)
    )

    val dataDS: DataStream[ProductInfo] = env.fromCollection(data)


    val kafkaSink = new FlinkKafkaProducer011[String](
      prop.getProperty("bootstrap.servers"),            // broker list
      "eventsource_yyb_product",                  // target topic
      new SimpleStringSchema());   // serialization schema
    kafkaSink.setWriteTimestampToKafka(true)

    dataDS.map(JSON.toJSON(_).toString).addSink(kafkaSink)


    env.execute("SendData2KafkaByKafkaConnectorBrowse")
  }
}
