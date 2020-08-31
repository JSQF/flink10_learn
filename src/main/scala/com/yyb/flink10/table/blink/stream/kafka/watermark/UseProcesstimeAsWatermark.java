package com.yyb.flink10.table.blink.stream.kafka.watermark;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.Current1;
import com.yyb.flink10.commonEntity.Current2;
import com.yyb.flink10.commonEntity.Rate;
import com.yyb.flink10.commonEntity.Rate2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

/**
 * @Author yyb
 * @Description kafka 作为 流式维度表 ok
 * @Date Create in 2020-07-27
 * @Time 16:59
 */
public class UseProcesstimeAsWatermark {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);

        //注意配置这里，可能导致 其他的问题
//        TableConfig tConfig = blinkTableEnv.getConfig();
//        tConfig.setIdleStateRetentionTime(org.apache.flink.api.common.time.Time.minutes(1), org.apache.flink.api.common.time.Time.minutes(6));

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

        /**
         *  Orders kafka
         */
        FlinkKafkaConsumer011<String> kafkaSourceOrder = new FlinkKafkaConsumer011<String>("eventsource_yyb_order", new SimpleStringSchema(), properties);
        DataStream<String> streamOrderStr = env.addSource(kafkaSourceOrder);
        DataStream<Current2> streamOrder = streamOrderStr.map(new MapFunction<String, Current2>() {
            @Override
            public Current2 map(String value) throws Exception {
                Current2 current = JSON.parseObject(value, Current2.class);
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(current.getRowtime(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                current.setEventTime(eventTimeTimestamp);
                return current;
            }
        }).assignTimestampsAndWatermarks(new TimestampExtractorOrder());

//        oederDS.print().setParallelism(1);
        Table orders = blinkTableEnv.fromDataStream(streamOrder, "rowtime,amount,currency,user_action_time.rowtime");
        blinkTableEnv.registerTable("Orders", orders);

        blinkTableEnv.toAppendStream(orders, Row.class).print().setParallelism(1);

        env.execute("UseProcesstimeAsWatermark");

    }


    static class TimestampExtractorOrder implements AssignerWithPeriodicWatermarks<Current2> {

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //使用 processTime 作为 waterMark, 经过测试 可以使用 processTime 作为 水印的
            return new Watermark(new Date().getTime());
        }

        @Override
        public long extractTimestamp(Current2 element, long previousElementTimestamp) {
            return element.getEventTime();
        }
    }


}
