package com.yyb.flink10.table.blink.stream.CDC.Mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-10-28
 * @Time 09:16
 */
public class FlinkMysqslCdc {
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

        env.getConfig().setAutoWatermarkInterval(1000);

        String sql = "create table visitlog (" +
                "visit_time timestamp," +
                "user_id bigint," +
                "user_name string," +
                "sex integer," +
                "age integer," +
                "ext_float float," +
                "ext_double double," +
                "ext_decimal decimal(6,2)," +
                "ext_date date," +
                "ext_time time," +
                "ext_smallint integer," +
                "ext_tinyint integer" +
                ") with ( " +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '172.16.10.114'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = 'qLdXd@rcd2mq'," +
                "'database-name' = 'canal_test'," +
                "'table-name' = 'visitlog'" +
                ")";

        blinkTableEnv.executeSql(sql);

        String sql1 = "select * from visitlog";
        Table t = blinkTableEnv.sqlQuery(sql1);
        DataStream<Tuple2<Boolean, RowData>> rs = blinkTableEnv.toRetractStream(t, RowData.class);
        rs.print().setParallelism(1);
        env.execute();


    }
}
