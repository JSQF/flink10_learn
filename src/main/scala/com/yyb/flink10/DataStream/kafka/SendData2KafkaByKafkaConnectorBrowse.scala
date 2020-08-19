package com.yyb.flink10.DataStream.kafka

import java.io.InputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.yyb.flink10.commonEntity.UserBrowseLog
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
object SendData2KafkaByKafkaConnectorBrowse {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val in_env: InputStream = ClassLoader.getSystemResourceAsStream("env.properties")
    val prop: Properties = new Properties()
    prop.load(in_env)

    val data = Array(
      new UserBrowseLog("user_1", "2016-01-01 00:00:00", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_1", "2016-01-01 00:00:01", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_1", "2016-01-01 00:00:02", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_1", "2016-01-01 00:00:03", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_1", "2016-01-01 00:00:04", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_1", "2016-01-01 00:00:05", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_1", "2016-01-01 00:00:06", "browse", "product_5", 20, 0L),
      new UserBrowseLog("user_2", "2016-01-01 00:00:01", "browse", "product_3", 20, 0L),
      new UserBrowseLog("user_2", "2016-01-01 00:00:02", "browse", "product_3", 20, 0L),
      new UserBrowseLog("user_2", "2016-01-01 00:00:05", "browse", "product_3", 20, 0L),
      new UserBrowseLog("user_2", "2016-01-01 00:00:06", "browse", "product_3", 20, 0L)
    )

    val dataDS: DataStream[UserBrowseLog] = env.fromCollection(data)


    val kafkaSink = new FlinkKafkaProducer011[String](
      prop.getProperty("bootstrap.servers"),            // broker list
      "eventsource_yyb_browse",                  // target topic
      new SimpleStringSchema());   // serialization schema
    kafkaSink.setWriteTimestampToKafka(true)

    dataDS.map(JSON.toJSON(_).toString).addSink(kafkaSink)


    env.execute("SendData2KafkaByKafkaConnectorBrowse")
  }
}
