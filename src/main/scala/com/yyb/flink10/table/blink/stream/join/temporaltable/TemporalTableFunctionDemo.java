package com.yyb.flink10.table.blink.stream.join.temporaltable;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.ProductInfo;
import com.yyb.flink10.commonEntity.UserBrowseLog;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-19
 * @Time 08:57
 */
public class TemporalTableFunctionDemo {
    public static void main(String [] args) throws Exception {
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

        FlinkKafkaConsumer011<String> kafkaSourceBrowse = new FlinkKafkaConsumer011<String>("eventsource_yyb_browse", new SimpleStringSchema(), properties);
        DataStream<UserBrowseLog> browseStream = env.addSource(kafkaSourceBrowse)
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseTimestampExtractor(Time.seconds(0)));
        blinkTableEnv.registerDataStream("browse",browseStream,"userID,eventTime,eventTimeTimestamp,eventType,productID,productPrice,browseRowtime.rowtime");


        DataStream<ProductInfo> productInfoStream=env
                .addSource(new FlinkKafkaConsumer011<>("eventsource_yyb_product", new SimpleStringSchema(), properties))
                .process(new ProductInfoProcessFunction())
                .assignTimestampsAndWatermarks(new ProductInfoTimestampExtractor(Time.seconds(0)));

        blinkTableEnv.registerDataStream("productInfo",productInfoStream, "productID,productName,productCategory,updatedAt,updatedAtTimestamp,productInfoRowtime.rowtime");
        //设置Temporal Table的时间属性和主键
        TemporalTableFunction productInfo = blinkTableEnv.scan("productInfo").createTemporalTableFunction("productInfoRowtime", "productID");
        //注册TableFunction
        blinkTableEnv.registerFunction("productInfoFunc",productInfo);

        String sql = ""
                + "SELECT "
                + "browse.userID, "
                + "browse.eventTime, "
                + "browse.eventTimeTimestamp, "
                + "browse.eventType, "
                + "browse.productID, "
                + "browse.productPrice, "
                + "productInfo.productID, "
                + "productInfo.productName, "
                + "productInfo.productCategory, "
                + "productInfo.updatedAt, "
                + "productInfo.updatedAtTimestamp "
                + "FROM "
                + " browse, "
                + " LATERAL TABLE (productInfoFunc(browse.browseRowtime)) as productInfo "
                + "WHERE "
                + " browse.productID=productInfo.productID";

        Table table = blinkTableEnv.sqlQuery(sql);
        blinkTableEnv.toAppendStream(table, Row.class).print();

        //6、开始执行
        blinkTableEnv.execute(TemporalTableFunctionDemo.class.getSimpleName());


    }

    static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog>{

        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {
                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getEventTime(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setEventTimeTimestamp(eventTimeTimestamp);
                out.collect(log);
            }catch (Exception ex){
                System.out.println("解析Kafka数据异常..." );
            }
        }
    }
    static class ProductInfoProcessFunction extends ProcessFunction<String, ProductInfo> {

        @Override
        public void processElement(String value, Context ctx, Collector<ProductInfo> out) throws Exception {
            try {

                ProductInfo log = JSON.parseObject(value, ProductInfo.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getUpdatedAt(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setUpdatedAtTimestamp(eventTimeTimestamp);

                out.collect(log);
            }catch (Exception ex){
                System.out.println("解析Kafka数据异常...");
            }
        }
    }
    static class BrowseTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {
            return element.getEventTimeTimestamp();
        }
    }
    static class ProductInfoTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ProductInfo> {

        ProductInfoTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(ProductInfo element) {
            return element.getUpdatedAtTimestamp();
        }
    }
}
