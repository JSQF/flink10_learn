package com.yyb.flink10.table.blink.stream.kafka;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.Current1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-10
 * @Time 18:04
 */
public class EventTimeDemo {
    public static void main(String[] args) throws Exception {
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

        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>("eventsource_yyb", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(kafkaSource);


        DataStream<Current1> currentDS = stream.process(new ProcessFunction<String, Current1>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Current1> out) throws Exception {
                Current1 current1 = JSON.parseObject(value, Current1.class);
                out.collect(current1);
            }
        });

        currentDS.assignTimestampsAndWatermarks(new TimestampExtractor(Time.seconds(0)));

        currentDS.print().setParallelism(1);

        // sql rowtime
        //注意 第一个 rowtime 是自己的 rowtime，user_action_time.rowtime才是 真正的 eventTime
        Table t = blinkTableEnv.fromDataStream(currentDS, "rowtime,amount,currency,user_action_time.rowtime");

        DataStream<Row> tRow = blinkTableEnv.toAppendStream(t, Row.class);
        tRow.print().setParallelism(1);
        env.execute("EventTimeDemo");


    }

    static class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Current1> {

        public TimestampExtractor(Time maxOutOfOrderness){
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(Current1 element) {
            return Long.parseLong(element.getRowtime());
        }
    }
}
