package com.yyb.flink10.table.blink.stream.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.Current2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.Elasticsearch7UpsertTableSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @Author yyb
 * @Description OK，列名 可以指定 但是 不支持 es 的用户名和密码
 * @Date Create in 2020-08-20
 * @Time 09:40
 */
public class WriteData2EsByConnectorTestEnv {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3000);
        env.disableOperatorChaining();

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
        }).assignTimestampsAndWatermarks(new TimestampExtractorOrder(Time.seconds(0)));

//        oederDS.print().setParallelism(1);
        Table orders = blinkTableEnv.fromDataStream(streamOrder, "rowtime,amount,currency,user_action_time.rowtime");
        blinkTableEnv.registerTable("Orders", orders);

        /**
         * 注意 cast 函数可以把  TimeStamp 转化为 long 型的 时间，但此时的 是 精确到 秒的，而不是 毫秒的。
         * CAST(user_action_time as bigint)
         */
        Table tsTable = blinkTableEnv.sqlQuery("select rowtime ,amount,currency, CAST(user_action_time as bigint) * 1000 user_action_time  from Orders");
        tsTable.printSchema();
        DataStream<Row> orderPC = blinkTableEnv.toAppendStream(tsTable, Row.class);
        orderPC.print().setParallelism(1);


        /**
         * ES Table connector
         */

        String esConnectorSQl = "CREATE TABLE ESSink(" +
                "rowtime varchar, "+
                "amount int, "+
                "currency varchar, "+
                "user_action_time bigint "+
                ")WITH(" +
                "'connector.type' = 'elasticsearch'," +
                "'connector.version' = '7'," +
                "'connector.hosts' = 'http://172.16.11.104:9200;http://172.16.11.66:9200;http://172.16.11.67:9200'," +
                "'connector.index' = 'flink_sink_dev'," +
                "'connector.document-type' = 'user'," +
                "'update-mode' = 'append'," +
                "'connector.key-delimiter' = '_'," +
                "'connector.key-null-literal' = 'n/a'," +
                "'connector.flush-on-checkpoint' = 'true'," +
                "'connector.bulk-flush.max-actions' = '42'," +
                "'connector.bulk-flush.backoff.max-retries' = '3'," +
                "'format.type' = 'json'" +
                ")" ;

        System.out.println(esConnectorSQl);
        blinkTableEnv.sqlUpdate(esConnectorSQl);

        blinkTableEnv.insertInto("ESSink", tsTable);



        env.execute("WriteData2EsBySink");


    }

    static class TimestampExtractorOrder extends BoundedOutOfOrdernessTimestampExtractor<Current2> {

        public TimestampExtractorOrder(Time maxOutOfOrderness){
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(Current2 element) {
            return element.getEventTime();
        }
    }
}
