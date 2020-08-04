package com.yyb.flink10.table.flink.stream.JDBC.InsetMode;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-04
 * @Time 14:57
 */
public class RetractStream {
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
        Table rs = flinkTableEnv.sqlQuery("select user , count(url) url_ct from clicks group by user");
        //注意这里 使用的是 toRetractStream
        DataStream<Tuple2<Boolean, Tuple2<String, Long>>> rs_ds = flinkTableEnv.toRetractStream(rs, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        }));
        rs_ds.print().setParallelism(1);

        env.execute("RetractStream");

        /**
         * 结果
         * (true,(Liz,1))
         * (true,(Bob,1))
         * (false,(Bob,1))
         * (true,(Bob,2))
         * (true,(Mary,1))
         * (false,(Mary,1))
         * (true,(Mary,2))
         *
         * 第一个元素为true表示这条数据为要插入的新数据，false表示需要删除的一条旧数据。
         * 也就是说可以把更新表中某条数据分解为先删除一条旧数据再插入一条新数据。
         */

    }
}
