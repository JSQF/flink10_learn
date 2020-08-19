package com.yyb.flink10.table.blink.stream.kafka;

import com.yyb.flink10.commonEntity.Current1;
import com.yyb.flink10.commonEntity.Rate;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-03
 * @Time 08:53
 */
public class WriteToKafkaByKafkaConnectorOfRates {
    public static void main(String [] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);
        InputStream in_env = ClassLoader.getSystemResourceAsStream("env.properties");
        Properties prop = new Properties();
        prop.load(in_env);
        System.out.println(prop.getProperty("zookeeper.connect"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Kafka kafka = new Kafka();
        kafka.version("0.11")
                .topic("eventsource_yyb_rate")
                .property("zookeeper.connect", prop.getProperty("zookeeper.connect"))
                .property("bootstrap.servers", prop.getProperty("bootstrap.servers")).
                property("group.id", "yyb_dev")
                .startFromLatest();

        Schema schema = new Schema();
        TableSchema tableSchema1 = TableSchema.builder()
                .field("rowtime", DataTypes.STRING())
                .field("currency", DataTypes.STRING())
                .field("rate", DataTypes.INT())
                .build();
        schema.schema(tableSchema1);
        ConnectTableDescriptor tableSource = blinkTableEnv.connect(kafka)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema);
        tableSource.createTemporaryTable("Rates");

        ArrayList data = new ArrayList();
        data.add(new Rate(new Date().getTime() + "", "Euro", 120));
        data.add(new Rate(new Date().getTime() + "", "Euro", 121));

        DataStreamSource dataDS = env.fromCollection(data);
        Table dataTable = blinkTableEnv.fromDataStream(dataDS);
        blinkTableEnv.registerTable("source", dataTable);

        String sql = "insert into Rates select rowtime,currency,rate from source";

        blinkTableEnv.sqlUpdate(sql);

        env.execute("WriteToKafkaByKafkaConnector");
    }




}
