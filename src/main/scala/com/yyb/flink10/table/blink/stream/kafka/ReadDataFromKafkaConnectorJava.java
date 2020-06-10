package com.yyb.flink10.table.blink.stream.kafka;


import com.yyb.flink10.commonEntity.Pi;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-06-10
  * @Time 09:32
  */
public class ReadDataFromKafkaConnectorJava {
  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment flinkTableEnv = StreamTableEnvironment.create(env, settings);

    Kafka kafka = new Kafka();
    kafka.version("0.11")
            .topic("eventsource_yhj")
            .property("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181")
            .property("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092")
            .property("group.id", "yyb_dev")
            .startFromEarliest();

    Schema schema = new Schema();
    TableSchema tableSchema = TableSchema.builder()
            .field("id", DataTypes.STRING())
            .field("time", DataTypes.STRING())
            .build();
    schema.schema(tableSchema);
    ConnectTableDescriptor tableSource = flinkTableEnv.connect(kafka)
            .withFormat(new Json().failOnMissingField(true))
            .withSchema(schema);
    tableSource.createTemporaryTable("test");
    String sql = "select * from test";

    Table test = flinkTableEnv.from("test");
    test.printSchema();


    DataStream<Pi> testDataStream = flinkTableEnv.toAppendStream(test, Pi.class);

    testDataStream.print().setParallelism(1);

    flinkTableEnv.execute("ReadDataFromKafkaConnector");
  }

}
