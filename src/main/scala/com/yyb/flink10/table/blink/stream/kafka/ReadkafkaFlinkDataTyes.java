package com.yyb.flink10.table.blink.stream.kafka;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.User;
import com.yyb.flink10.util.JsonDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import java.io.InputStream;
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
        properties.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        properties.setProperty("zookeeper.connect", prop.getProperty("zookeeper.connect"));
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>("eventsource_order_yyb", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(kafkaSource);
        SingleOutputStreamOperator<User> userStream = stream.map(new MapFunction<String, User>() {
            @Override
            public User map(String s) throws Exception {
                return JSON.parseObject(s, User.class);
            }
        });

        userStream.print();
        Table t = blinkTableEnv.fromDataStream(userStream);
        blinkTableEnv.createTemporaryView("t", t);
        String sql = "select user_id, count(*) / 100.0 cnt_avg from t group by user_id";
        Table t1 = blinkTableEnv.sqlQuery(sql);
        t1.printSchema();
        DataType[] dataTypes = t1.getSchema().getFieldDataTypes();
        for(DataType dataType : dataTypes){
            System.out.println(dataType.toString());
        }


        env.execute();
    }
}
