package com.yyb.flink10.DataStream.broadCast;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.Current1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-11-19
 * @Time 10:26
 */
public class BroadCast {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getConfig().setAutoWatermarkInterval(1000);

        InputStream in_env  = ClassLoader.getSystemResourceAsStream("env.properties");
        Properties prop = new Properties();
        prop.load(in_env);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        properties.setProperty("zookeeper.connect", prop.getProperty("zookeeper.connect"));
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>("eventsource_yyb", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(kafkaSource);

        env.addSource(kafkaSource).broadcast();

        DataStream<Current1> currentDS = stream.process(new ProcessFunction<String, Current1>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Current1> out) throws Exception {
                Current1 current1 = JSON.parseObject(value, Current1.class);
                out.collect(current1);
            }
        });


    }
}
