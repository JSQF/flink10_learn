package com.yyb.flink10.table.blink.stream.kafka;

import com.yyb.flink10.commonEntity.Pi;
import com.yyb.flink10.commonEntity.Rate2;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-03
 * @Time 08:53
 */
public class WriteToKafkaByKafkaConnectorOfPi {
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
                .topic("eventsource_yyb_pi")
                .property("zookeeper.connect", prop.getProperty("zookeeper.connect"))
                .property("bootstrap.servers", prop.getProperty("bootstrap.servers")).
                property("group.id", "yyb_dev")
                .startFromLatest();

        Schema schema = new Schema();
        TableSchema tableSchema1 = TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("time", DataTypes.STRING())
                .build();
        schema.schema(tableSchema1);
        ConnectTableDescriptor tableSource = blinkTableEnv.connect(kafka)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema);
        tableSource.createTemporaryTable("Pi");

        ArrayList<Tuple2<String, String>> data = new ArrayList<>();
        data.add(Tuple2.of("2016-01-01 00:00:02", "Euro"));

        DataStreamSource<Tuple2<String, String>> dataDS = env.fromCollection(data);
        dataDS.print().setParallelism(1);
        Table dataTable = blinkTableEnv.fromDataStream(dataDS, $("time"), $("id"));
        blinkTableEnv.registerTable("source", dataTable);
        dataTable.printSchema();

        String sql = "insert into Pi select * from source";

        blinkTableEnv.executeSql(sql);

        env.execute("WriteToKafkaByKafkaConnector");
    }




}
