package com.yyb.flink10.table.flink.stream.JDBC.InsetMode;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-04
 * @Time 14:57
 */
public class AppendOnly {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment flinkTableEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<String, String>> data = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );

        Table clicksTable = flinkTableEnv.fromDataStream(data, "user,url");

        flinkTableEnv.registerTable("clicks", clicksTable);
        Table rs = flinkTableEnv.sqlQuery("select user , url from clicks where user='Mary'");
        DataStream<Row> rs_ds = flinkTableEnv.toAppendStream(rs, Row.class);
        rs_ds.print().setParallelism(1);

        env.execute("AppendOnly");

        /**
         * 结果
         * Mary,./prod?id=1
         * Mary,./home
         */

    }
}
