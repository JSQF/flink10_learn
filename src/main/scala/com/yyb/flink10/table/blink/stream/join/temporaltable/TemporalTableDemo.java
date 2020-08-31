package com.yyb.flink10.table.blink.stream.join.temporaltable;

import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-07-27
 * @Time 16:14
 */
public class TemporalTableDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);

        JdbcLookupOptions lookOption = JdbcLookupOptions.builder()
                .setCacheExpireMs(60 * 1000)
                .setCacheMaxSize(1024 * 1024)
                .setMaxRetryTimes(10)
                .build();

        JdbcOptions jdbcOpition = JdbcOptions.builder()
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC")
                .setDriverName("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("111111")
                .setTableName("RatesHistory")
                .build();

        JdbcReadOptions jdbcReadOption = JdbcReadOptions.builder()
                .setFetchSize(5000)
                .build();

        TableSchema tableSchema = TableSchema.builder()
                .field("rowtime", new AtomicDataType(new VarCharType(2147483647)))
                .field("currency", new AtomicDataType(new VarCharType(2147483647))) //注意 String 就是 2147483647
                .field("rate", new AtomicDataType(new IntType()))
                .build();

        JdbcTableSource jdbcTableSource = JdbcTableSource.builder()
                .setLookupOptions(lookOption)
                .setOptions(jdbcOpition)
                .setReadOptions(jdbcReadOption)
                .setSchema(tableSchema)
                .build();

        blinkTableEnv.registerTableSource("LatestRates", jdbcTableSource);

        blinkTableEnv.registerFunction("jdbcLookup", jdbcTableSource.getLookupFunction(new String[]{"currency"}));

        // 注意不能直接 访问 时态表的数据
        String sql = "SELECT * FROM LatestRates FOR SYSTEM_TIME AS OF Timestamp '2020-07-27 16:30:15'";
        sql = "select * from LatestRates";
        Table rs1 = blinkTableEnv.sqlQuery(sql);
        DataStream<Row> rs1DataStream = blinkTableEnv.toAppendStream(rs1, Row.class);
        rs1DataStream.print().setParallelism(1);

        blinkTableEnv.execute("TemporalTableDemo");
    }
}
