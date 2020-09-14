package com.yyb.flink10.DataStream.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-04
 * @Time 14:43
 */
public class ReadkafkaFlinkDataTyes {
    public static void main(String[] args) throws Exception{
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getConfig().setAutoWatermarkInterval(1000);

        InputStream in_env  = ClassLoader.getSystemResourceAsStream("env.properties");
        Properties prop = new Properties();
        prop.load(in_env);



        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        properties.setProperty("bootstrap.servers", "172.16.11.17:9092,172.16.11.18:9092,172.16.11.19:9092");
//        properties.setProperty("zookeeper.connect", prop.getProperty("zookeeper.connect"));
        properties.setProperty("zookeeper.connect", "njtest-cdh6-nn01.nj:2181,njtest-cdh6-nn02.nj:2181,njtest-cdh6-nn03.nj:2181");
        properties.setProperty("group.id", "test");


        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>("as28-60p", new SimpleStringSchema(), properties);
        DataStreamSource<String> source = env.addSource(kafkaSource);
        source.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String str) throws Exception {
                System.out.println(Arrays.toString(str.getBytes()));
                System.out.println(DatatypeConverter.printHexBinary(str.getBytes()));
                System.out.println();
                    return null;
            }
        });

        env.execute("ReadkafkaFlinkDataTyes");


    }
}
