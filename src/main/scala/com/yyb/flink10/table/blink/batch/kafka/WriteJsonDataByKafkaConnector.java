package com.yyb.flink10.table.blink.batch.kafka;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-07-27
 * @Time 19:22
 */
public class WriteJsonDataByKafkaConnector {
    public static void main(String[] args){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(settings);

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
        ConnectTableDescriptor tableSink = blinkBatchTableEnv.connect(kafka)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema);


        tableSink.createTemporaryTable("kafka_sink");





    }
}
