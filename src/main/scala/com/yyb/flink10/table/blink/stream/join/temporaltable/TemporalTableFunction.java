package com.yyb.flink10.table.blink.stream.join.temporaltable;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author yyb
 * @Description 时态表 函数
 * @Date Create in 2020-07-27
 * @Time 15:44
 */
public class TemporalTableFunction {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, settings);
        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        DataStreamSource<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
        //加入 processtime 属性
        Table ratesHistory = blinkTableEnv.fromDataStream(ratesHistoryStream, "r_currency, r_rate, r_proctime.proctime");
        blinkTableEnv.createTemporaryView("RatesHistory", ratesHistory);

        //创建 Temporal Table Function
        org.apache.flink.table.functions.TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency");
        blinkTableEnv.registerFunction("Rates", rates);

        // 注意不能直接 访问 时态表函数的数据
        String sql = "SELECT * FROM RatesHistory FOR SYSTEM_TIME AS OF TIME '16:01:15'";

        Table rs1 = blinkTableEnv.sqlQuery(sql);
        DataStream<Row> rs1DataStream = blinkTableEnv.toAppendStream(rs1, Row.class);
        rs1DataStream.print().setParallelism(1);

        blinkTableEnv.execute("TemporalTableFunction");
    }
}
