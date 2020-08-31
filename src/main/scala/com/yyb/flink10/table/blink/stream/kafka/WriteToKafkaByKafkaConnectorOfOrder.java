package com.yyb.flink10.table.blink.stream.kafka;

import com.yyb.flink10.commonEntity.Current1;
import com.yyb.flink10.commonEntity.Current2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

/**
 * @Author yyb
 * @Description 注意 在 join的时候，是由 水印 触发的 （即 每当 新的水印大于 旧的水印 才会触发计算， join 的时候，由所有流中的 min 的水印决定 这个 join 的水印）
 * @Date Create in 2020-08-03
 * @Time 08:53
 */
public class WriteToKafkaByKafkaConnectorOfOrder {
    public static void main(String [] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);
        InputStream in_env = ClassLoader.getSystemResourceAsStream("env.properties");
        Properties prop = new Properties();
        prop.load(in_env);
        System.out.println(prop.getProperty("zookeeper.connect"));

        Kafka kafka = new Kafka();
        kafka.version("0.11")
                .topic("eventsource_yyb_order")
                .property("zookeeper.connect", prop.getProperty("zookeeper.connect"))
                .property("bootstrap.servers", prop.getProperty("bootstrap.servers")).
                property("group.id", "yyb_dev")
                .startFromLatest();
        Schema schema = new Schema();
        TableSchema tableSchema1 = TableSchema.builder()
                .field("rowtime", DataTypes.STRING())
                .field("amount", DataTypes.INT())
                .field("currency", DataTypes.STRING())
                .field("eventTime", DataTypes.BIGINT())
                .build();
        schema.schema(tableSchema1);
        ConnectTableDescriptor tableSource = blinkTableEnv.connect(kafka)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema);
        tableSource.createTemporaryTable("Orders");

        ArrayList data = new ArrayList();
        data.add(new Current2( "2016-01-01 00:00:00",3, "Euro", 0L));

        DataStreamSource dataDS = env.fromCollection(data);
        Table dataTable = blinkTableEnv.fromDataStream(dataDS);
        blinkTableEnv.registerTable("source", dataTable);

        String sql = "insert into Orders select rowtime, amount, currency, eventTime from source";

        blinkTableEnv.sqlUpdate(sql);

        env.execute("WriteToKafkaByKafkaConnector");
    }




}
