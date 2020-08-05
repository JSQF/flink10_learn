package com.yyb.flink10.table.blink.stream.kafka

import java.util
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSourceSinkFactory
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment


/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-06-09
  * @Time 22:21
  */
object ReadDataFromKafkaSource {
  def main(args:Array[String])={
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val flinkTableEnv = StreamTableEnvironment.create(env, settings)

    val kafkaSourceFactory = new Kafka010TableSourceSinkFactory()
    val proper = new util.HashMap[String, String]()
    val kafkaSource = kafkaSourceFactory.createStreamTableSource(proper)

    flinkTableEnv.registerTableSource( "kafka", kafkaSource)

    env.execute("")
  }
}
