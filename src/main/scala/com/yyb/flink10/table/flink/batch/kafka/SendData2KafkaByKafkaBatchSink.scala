package com.yyb.flink10.table.flink.batch.kafka

import java.io.InputStream
import java.util.Properties

import com.yyb.flink10.OutputFormat.KafkaOutputFormat
import com.yyb.flink10.sink.KafkaBatchTableSink
import org.apache.flink.api.scala._
import org.apache.flink.formats.json.JsonRowSerializationSchema
import org.apache.flink.kafka011.shaded.org.apache.kafka.clients.producer.ProducerRecord
import org.apache.flink.kafka011.shaded.org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment, TableSchema}
import org.apache.flink.table.descriptors.{ConnectTableDescriptor, Json, Kafka, Schema}


/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-07-28
  * @Time 16:12
  */
object SendData2KafkaByKafkaBatchSink {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val blinkTableEnv = BatchTableEnvironment.create(env)
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

    val schemaString = new JsonRowSerializationSchema.Builder(tableSchema1.toRowType)
    val kafkaProp = new Properties();
    kafkaProp.put("key.serializer", classOf[StringSerializer])
    kafkaProp.put("value.serializer", classOf[StringSerializer])
    kafkaProp.put("zookeeper.connect", prop.getProperty("zookeeper.connect"))
    kafkaProp.put("bootstrap.servers", prop.getProperty("bootstrap.servers"))
    kafkaProp.put("topic", "eventsource_yhj")

    val kafkaProducer = new FlinkKafkaProducer[String, String](kafkaProp)
    val data = Array(Current(1, "Euro"))

    val dataDS = env.fromCollection(data)

    val datasOfRecord: DataSet[ProducerRecord[String, String]] =  dataDS.map(x => {
       val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("eventsource_yhj", x.toString)
      record
    })




    /**
      * 这里的 发送数据 到 kafka是 先 collect 到 driver 才 发送的，所以不是 分布式的处理方法
      * 需要调优
      */
//    datasOfRecord.collect().foreach(kafkaProducer.send(_))
//    kafkaProducer.flush()

    /**
      * 这里使用的是 dataset 的 kafkaOutputFormat
      */
    val kafkaOutputFormat = new KafkaOutputFormat(kafkaProp);
//    dataDS.map(x => x.toString).output(kafkaOutputFormat)

    val dataTable: Table = blinkTableEnv.fromDataSet(dataDS.map(_.toString))

    blinkTableEnv.registerTable("dataSource", dataTable)

    val kafkaBatchTableSink = new KafkaBatchTableSink(kafkaOutputFormat);
    blinkTableEnv.registerTableSink("kafkaBatchTableSink", kafkaBatchTableSink)

    var sql =
      """
        |insert into kafkaBatchTableSink select * from dataSource
        |""".stripMargin
    //因为 kafka 是 无界的， 所以不能使用 batch 模式 的 kafkatablesink
    //BatchTableSink or OutputFormatTableSink required to emit batch Table.
    blinkTableEnv.sqlUpdate(sql)

//    dataTable.insertInto("Orders_tmp")


    env.execute("SendData2KafkaByKafkaConnector")
  }

  case class Current(amount:Int, currency:String){
    override def toString: String = {
      s"""{"amount":"${amount}",currency:"${currency}"}""".stripMargin
    }

    def toBytes(): Array[Byte] ={
      toString.getBytes()
    }
  }
}
