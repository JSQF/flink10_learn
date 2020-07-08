package com.yyb.flink10.table.blink.stream.hive

import java.util.Properties

import com.yyb.flink10.commonEntity.Pi
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.formats.json.JsonRowDeserializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.descriptors.Schema

/**
  * @Author yyb
  * @Description 注意使用 java 版本的代码
  *             本代码 在 new RowTypeInfo 的时候报错
  * @Date Create in 2020-07-07
  * @Time 10:26
  */
object WriteData2HiveReadFromkafkaTableSource {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val schema = new Schema()
    val tableSchema: TableSchema =  TableSchema.builder()
      .field("id", DataTypes.STRING())
      .field("time", DataTypes.STRING())
      .build()
    schema.schema(tableSchema)
    val prop = new Properties()
    prop.put("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181")
    prop.put("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092")
    prop.put("group.id", "yyb_dev")

    val types: Array[BasicTypeInfo[String]] =  Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val fields: Array[String] = Array("id", "time")
//    val rowTypeINfo = new RowTypeInfo(types, fields)
    val jsonRowDeserializationSchema = new JsonRowDeserializationSchema.Builder(schema.toString).build()

    val kafka = new Kafka010TableSource(tableSchema, "eventsource_yhj", prop, jsonRowDeserializationSchema)

    val kafkaSource: Table = tableEnv.fromTableSource(kafka)
    tableEnv.createTemporaryView("kafkaSource", kafkaSource)


    val name = "myhive"
    val defaultDatabase = "flink"
    val hiveConfDir = this.getClass.getResource("/").getFile  //可以通过这一种方式设置 hiveConfDir，这样的话，开发与测试和生产环境可以保持一致

//    val version = "2.3.6"
    val version = "1.1.0"
    val hive = new  HiveCatalog(name, defaultDatabase, hiveConfDir, version)

    tableEnv.registerCatalog("myhive", hive)
    tableEnv.useCatalog("myhive")

    var sql =
      s"""
         |insert into table myhive.${defaultDatabase}.a select * from kafkaSource
         |""".stripMargin
    tableEnv.sqlUpdate(sql)

    val kafkaSourceDataStream: DataStream[Pi] = tableEnv.toAppendStream[Pi](kafkaSource)
    kafkaSourceDataStream.print().setParallelism(1)

    tableEnv.execute("WriteData2Hive")

  }
}
