package com.yyb.flink10.table.blink.stream.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.Current2;
import com.yyb.flink10.table.blink.stream.join.temporaltable.JoinWithKafkaConsumerTeporalTableFunction;
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
import org.apache.flink.streaming.connectors.Elasticsearch7UpsertTableSinkPlus;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6UpsertTableSink;
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
 * @Description
 * @Date Create in 2020-08-20
 * @Time 09:40
 */
public class WriteData2EsBySink {
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
        DataStream<Row> orderPC = blinkTableEnv.toAppendStream(blinkTableEnv.sqlQuery("select *, 'Orders' from Orders"), Row.class);
        orderPC.print().setParallelism(1);


        /**
         * ES Table Sink
         */
        List<ElasticsearchUpsertTableSinkBase.Host> hosts= new ArrayList<>();
        String ESProtocol = prop.getProperty("es.protocol");
        String esHosts = prop.getProperty("es.hosts");
        String esUser = prop.getProperty("es.username");
        String esPassword = prop.getProperty("es.password");
        for(String host : esHosts.split(",")){
            String[] hostStr = host.split(":");
            hosts.add(new ElasticsearchUpsertTableSinkBase.Host(hostStr[0], Integer.parseInt(hostStr[1]), ESProtocol));
        }


        RowTypeInfo typeInfo = new RowTypeInfo(orders.getSchema().getFieldTypes());

        JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema.Builder(typeInfo).build();

        Map<ElasticsearchUpsertTableSinkBase.SinkOption, String> sinkOptions = new HashMap<>();



        Elasticsearch7UpsertTableSinkPlus ES7UpsertTableSink = new Elasticsearch7UpsertTableSinkPlus(
                true,
                orders.getSchema(),
                hosts,
                "flink_sink_dev",
                "_",
                "n/a",
                serializationSchema,
                XContentType.JSON,
                new NoOpFailureHandler(),
                sinkOptions,
                esUser,
                esPassword
        );

        blinkTableEnv.registerTableSink("ESSink", ES7UpsertTableSink);

        blinkTableEnv.insertInto("ESSink", orders);

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
