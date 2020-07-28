package com.yyb.flink10.table.blink.stream.join.temporaltable;

import org.apache.flink.api.java.io.jdbc.JDBCLookupOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCReadOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
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
 * @Description
 * @Date Create in 2020-07-27
 * @Time 16:59
 */
public class JoinWithTeporalTableFunctionWithJDBCConnection {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);
        JDBCLookupOptions lookOption = JDBCLookupOptions.builder()
                .setCacheExpireMs(60 * 1000)
                .setCacheMaxSize(1024 * 1024)
                .setMaxRetryTimes(10)
                .build();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String mysqlSQL = "CREATE TABLE xxx ( currency varchar ) with ( 'connector.type' = 'jdbc','connector.url'='jdbc:mysql://localhost:3306/test,'connector.table'='RateHistory','connector.driver'='com.mysql.jdbc.Driver','connector.username'='root','connector.password'='111111')";
//               String a1 =  "currency varchar(20), " +
//                "rate int(11) ) WITH (" +
//                "'connector.type' = 'jdbc'," +
//                "'connector.url' = 'jdbc:mysql://localhost:3306/test'" +
//                "'connector.table' = 'RateHistory'," +
//                "'connector.driver' = 'com.mysql.jdbc.Driver'," +
//                "'connector.username' = 'root'," +
//                "'connector.password' = '111111' " +
//                ")";

         final String TABLE_SOURCE_SQL = "CREATE TABLE rate (" +
                " rowtime varchar, " +
                " currency varchar, " +
                " rate int " +
                ") with (" +
                " 'connector.type' = 'jdbc', " +
                " 'connector.url' = 'jdbc:mysql://localhost:3306/test', " +
                " 'connector.table' = 'RatesHistory', " +
                " 'connector.driver' = 'com.mysql.jdbc.Driver' ," +
                " 'connector.lookup.cache.max-rows' = '5000' ," +
                " 'connector.lookup.cache.ttl' = '2s' ," +
                " 'connector.username' = 'root' ," +
                " 'connector.password' = '111111' " +
                ")";

        blinkTableEnv.sqlUpdate(TABLE_SOURCE_SQL);
        // jdbc temportal table end

        String sql = "select * from rate";
        Table a = blinkTableEnv.sqlQuery(sql);
        DataStream<Row> jdbcSourceDataStream = blinkTableEnv.toAppendStream(a, Row.class);
        jdbcSourceDataStream.print().setParallelism(1);

        Table ratesHistory = blinkTableEnv.fromDataStream(jdbcSourceDataStream, "currency, rate, proctime.proctime");
        blinkTableEnv.createTemporaryView("RatesHistory", ratesHistory);
        org.apache.flink.table.functions.TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("proctime", "currency");
        blinkTableEnv.registerFunction("Rates", rates);


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
        oederDS.print().setParallelism(1);
        Table orders = blinkTableEnv.fromDataStream(oederDS, "amount,currency,proctime.proctime");
        blinkTableEnv.registerTable("Orders", orders);

        String sql1 = "select o.currency,o.amount,r.rate,o.amount * r.rate AS amount from Orders as o , LATERAL TABLE (Rates(o.proctime)) as r WHERE o.currency = r.currency";
        Table rs1 = blinkTableEnv.sqlQuery(sql1);
        DataStream<Row> rs1DS = blinkTableEnv.toAppendStream(rs1, Row.class);
        rs1DS.print().setParallelism(1);

        env.execute("JoinWithTeporalTableFunction");



    }
}
