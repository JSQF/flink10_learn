package com.yyb.flink10.table.blink.stream.join.temporaltable;

import org.apache.flink.api.java.io.jdbc.JDBCLookupOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCReadOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description lookupFunction 作为 维度表 和 checkpoint 结合 是没有问题的
 * @Date Create in 2020-07-27
 * @Time 16:59
 */
public class JoinWithLookupFunctionCheckpoint {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);
        JDBCLookupOptions lookOption = JDBCLookupOptions.builder()
                .setCacheExpireMs(0)
                .setCacheMaxSize(0)
                .setMaxRetryTimes(3)
                .build();

        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // jdbc temportal table start
        JDBCOptions jdbcOpition = JDBCOptions.builder()
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
                .setDriverName("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("111111")
                .setTableName("RatesHistory")
                .build();

        JDBCReadOptions jdbcReadOption = JDBCReadOptions.builder()
//                .setFetchSize(0)
                .build();

        TableSchema tableSchema = TableSchema.builder()
                .field("rowtime", new AtomicDataType(new VarCharType(2147483647)))
                .field("currency", new AtomicDataType(new VarCharType(2147483647))) //注意 String 就是 2147483647
                .field("rate", new AtomicDataType(new IntType()))
                .build();

        JDBCTableSource jdbcTableSource = JDBCTableSource.builder()
                .setLookupOptions(lookOption)
                .setOptions(jdbcOpition)
                .setReadOptions(jdbcReadOption)
                .setSchema(tableSchema)
                .build();


        blinkTableEnv.registerFunction("jdbcLookup", jdbcTableSource.getLookupFunction(new String[]{"currency"}));
        // jdbc temportal table end



        Kafka kafka = new Kafka();
        kafka.version("0.11")
                .topic("eventsource_yhj")
                .property("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181")
                .property("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092")
                .property("group.id", "yyb_dev")
                .startFromLatest();

        Schema schema = new Schema();
        TableSchema tableSchema1 = TableSchema.builder()
                .field("amount", DataTypes.INT())
                .field("currency", DataTypes.STRING())
                .build();
        schema.schema(tableSchema1);
        ConnectTableDescriptor tableSource  =  blinkTableEnv.connect(kafka)
                .withFormat( new Json().failOnMissingField(true) )
                .withSchema(schema);
        tableSource.createTemporaryTable("Orders_tmp");

        String sql_order = "select * from Orders_tmp";
        Table order = blinkTableEnv.sqlQuery(sql_order);
        DataStream<Row> oederDS = blinkTableEnv.toAppendStream(order, Row.class);
//        oederDS.print().setParallelism(1);
        Table orders = blinkTableEnv.fromDataStream(oederDS, "amount,currency,proctime.proctime");
        blinkTableEnv.registerTable("Orders", orders);
        DataStream<Row> orderPC = blinkTableEnv.toAppendStream(blinkTableEnv.sqlQuery("select *, '---' from Orders"), Row.class);
        orderPC.print().setParallelism(1);

        /**
         * 注意 使用 temporal table 作为 维表的时候，当维表 有更新的时候，temporal table 不会更新的
         *
         * 但是 使用 lookup function 的时候 是可以更新的 (不能配置 缓存时间 和 条数)， 但是就不是 temporal table 了
         */
        String sql1 = "select o.currency,o.amount,r.rate,o.amount * r.rate AS amount from Orders as o , LATERAL TABLE (Rates(o.proctime)) as r WHERE o.currency = r.currency";
        sql1 = "select o.currency,o.amount,r.rate,o.amount * r.rate , 'hahaha' AS amount from  Orders as o , LATERAL TABLE (jdbcLookup(o.currency)) as r"; //lookup function
        Table rs1 = blinkTableEnv.sqlQuery(sql1);
        DataStream<Row> rs1DS = blinkTableEnv.toAppendStream(rs1, Row.class);
        rs1DS.print().setParallelism(1);

        blinkTableEnv.execute("JoinWithTeporalTableFunction");



    }
}
