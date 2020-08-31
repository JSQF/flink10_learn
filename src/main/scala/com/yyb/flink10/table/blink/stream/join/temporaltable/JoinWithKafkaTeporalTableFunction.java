package com.yyb.flink10.table.blink.stream.join.temporaltable;

import com.yyb.flink10.commonEntity.Current1;
import com.yyb.flink10.commonEntity.Rate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author yyb
 * @Description kafka 作为 流式维度表 注意这里不能 通过 kafkaTableSource 转 dataStream ，加 waterMark ，再加 rowtime，flink 会报 空指针 错误
 * 请使用 kafkaConsumer
 * @Date Create in 2020-07-27
 * @Time 16:59
 */
public class JoinWithKafkaTeporalTableFunction {
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


        /**
         *  Orders kafka
         */
//        Kafka kafka = new Kafka();
//        kafka.version("0.11")
//                .topic("eventsource_yyb_order")
//                .property("zookeeper.connect", prop.getProperty("zookeeper.connect"))
//                .property("bootstrap.servers", prop.getProperty("bootstrap.servers"))
//                .property("group.id", "yyb_dev")
//                .startFromLatest();
//
//        Schema schema = new Schema();
//        TableSchema tableSchema1 = TableSchema.builder()
//                .field("rowtime", DataTypes.STRING())
//                .field("amount", DataTypes.INT())
//                .field("currency", DataTypes.STRING())
//                .build();
//        schema.schema(tableSchema1);
//
//
//        ConnectTableDescriptor tableSource  =  blinkTableEnv.connect(kafka)
//                .withFormat( new Json().failOnMissingField(true) )
//                .withSchema(schema);
//
//        tableSource.createTemporaryTable("Orders_tmp");
//
//        String sql_order = "select  amount, currency, rowtime from Orders_tmp";
//        Table order = blinkTableEnv.sqlQuery(sql_order);
//        DataStream<Current1> oederDS = blinkTableEnv.toAppendStream(order, Current1.class);
//        oederDS.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks)new TimestampExtractorOrder(Time.seconds(0)));
////        oederDS.print().setParallelism(1);
//        Table orders = blinkTableEnv.fromDataStream(oederDS, "amount,currency,user_action_time.rowtime");
//        blinkTableEnv.registerTable("Orders", orders);
//        DataStream<Row> orderPC = blinkTableEnv.toAppendStream(blinkTableEnv.sqlQuery("select *, '---' from Orders"), Row.class);
//        orderPC.print().setParallelism(1);


        /**
         * Rates kafka
         */
        Kafka kafka_rate = new Kafka();
        kafka_rate.version("0.11")
                .topic("eventsource_yyb_rate")
                .property("zookeeper.connect", prop.getProperty("zookeeper.connect"))
                .property("bootstrap.servers", prop.getProperty("bootstrap.servers"))
                .property("group.id", "yyb_dev")
                .startFromLatest();

        Schema schema_rate = new Schema();
        TableSchema tableSchema_rate = TableSchema.builder()
                .field("rowtime", DataTypes.STRING())
                .field("currency", DataTypes.STRING())
                .field("rate", DataTypes.INT())
                .build();
        schema_rate.schema(tableSchema_rate);
        ConnectTableDescriptor tableSource_rate  =  blinkTableEnv.connect(kafka_rate)
                .withFormat( new Json().failOnMissingField(true) )
                .withSchema(schema_rate);
        tableSource_rate.createTemporaryTable("rate_tmp");

        String sql_rate = "select currency, rate, rowtime from rate_tmp";
        Table rate = blinkTableEnv.sqlQuery(sql_rate);
        DataStream<Rate> rateDS = blinkTableEnv.toAppendStream(rate, Rate.class);
        rateDS.print().setParallelism(1);
        rateDS.assignTimestampsAndWatermarks(new TimestampExtractorRate(Time.seconds(0)));
        /**
         * 注意这里不能 通过 kafkaTableSource 转 dataStream ，加 waterMark ，再加 rowtime，flink 会报 空指针 错误
         */
        Table rates = blinkTableEnv.fromDataStream(rateDS, "currency,rate,user_action_time.rowtime");
        DataStream<Row> xx = blinkTableEnv.toAppendStream(rates, Row.class);
        xx.print().setParallelism(1);
//        blinkTableEnv.registerTable("Rates", rates);
//        DataStream<Row> ratePC = blinkTableEnv.toAppendStream(blinkTableEnv.sqlQuery("select *, '---' from Rates"), Row.class);
//        ratePC.print().setParallelism(1);
//
//        TemporalTableFunction ratesF = rates.createTemporalTableFunction("user_action_time", "currency");
//        blinkTableEnv.registerFunction("RatesF", ratesF);



        /**
         * 注意 使用 temporal table 作为 维表的时候，当维表 有更新的时候，temporal table 不会更新的
         *
         * 但是 使用 lookup function 的时候 是可以更新的 (不能配置 缓存时间 和 条数)， 但是就不是 temporal table 了
         */
//        String sql1 = "select o.currency,o.amount,r.rate,o.amount * r.rate AS amount, 'hahahah' from Orders as o , LATERAL TABLE (RatesF(o.user_action_time)) as r WHERE o.currency = r.currency";
//        Table rs1 = blinkTableEnv.sqlQuery(sql1);
//        DataStream<Row> rs1DS = blinkTableEnv.toAppendStream(rs1, Row.class);
//        rs1DS.print().setParallelism(1);

        blinkTableEnv.execute("JoinWithTeporalTableFunction");



    }


    static class TimestampExtractorOrder extends BoundedOutOfOrdernessTimestampExtractor<Current1> {

        public TimestampExtractorOrder(Time maxOutOfOrderness){
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(Current1 element) {
            return Long.parseLong(element.getRowtime());
        }
    }

    static class TimestampExtractorRate extends BoundedOutOfOrdernessTimestampExtractor<Rate> {

        public TimestampExtractorRate(Time maxOutOfOrderness){
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(Rate element) {
            System.out.println("here:" + element);
            return Long.parseLong(element.getRowtime());
        }
    }
}
