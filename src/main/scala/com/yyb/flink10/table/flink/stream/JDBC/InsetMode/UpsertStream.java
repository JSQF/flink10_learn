package com.yyb.flink10.table.flink.stream.JDBC.InsetMode;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-04
 * @Time 14:57
 */
public class UpsertStream {
    public static void main(String [] args) throws Exception {
        //注意第2个sql 需要使用 blink的 引擎
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment flinkTableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple2<String, String>> data = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );

        Table clicksTable = flinkTableEnv.fromDataStream(data, "user,url");

        flinkTableEnv.registerTable("clicks", clicksTable);
//        Table rs = flinkTableEnv.sqlQuery("select user , count(url) url_ct from clicks group by user");
        Table rs = flinkTableEnv.sqlQuery("select user, url_ct from ( select user , count(url) url_ct from clicks group by user ) order by url_ct limit 2");

        flinkTableEnv.registerTableSink("MemoryUpsertSink", new MemoryUpsertSink(rs.getSchema()));

        rs.insertInto("MemoryUpsertSink");

        env.execute("RetractStream");

        /**
         * select user , count(url) url_ct from clicks group by user
         * 结果
         * send message:(true,(Liz,1))
         * send message:(true,(Bob,1))
         * send message:(true,(Bob,2))
         * send message:(true,(Mary,1))
         * send message:(true,(Mary,2))
         *
         * 第一个元素为true表示这条数据为要插入的新数据，false表示需要删除的一条旧数据。
         * 也就是说可以把更新表中某条数据分解为先删除一条旧数据再插入一条新数据。
         *
         *select user, url_ct from ( select user , count(url) url_ct from clicks group by user ) order by url_ct limit 2
         * 结果
         * send message:(true,(Mary,1))
         * send message:(false,(Mary,1))
         * send message:(true,(Mary,2))
         * send message:(true,(Bob,1))
         * send message:(false,(Mary,2))
         * send message:(true,(Liz,1))
         * send message:(false,(Bob,1))
         * send message:(true,(Mary,2))
         *
         */

    }

    // 自己实现的 MemoryUpsertSink，目的是 看到 转化后的数据
    private static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple2<String, Long>>{
        private TableSchema schema;
        private String[] keyFields;
        private boolean isAppendOnly;

        private String[] fieldNames;
        private TypeInformation<?>[] fieldTypes;

        public MemoryUpsertSink() {

        }

        public MemoryUpsertSink(TableSchema schema) {
            this.schema = schema;
        }

        public void setSchema(TableSchema schema) {
            this.schema = schema;
        }

        public void setAppendOnly(boolean appendOnly) {
            isAppendOnly = appendOnly;
        }

        public void setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
            this.fieldTypes = fieldTypes;
        }

        @Override
        public void setKeyFields(String[] keys) {
            this.keyFields = keys;
        }

        @Override
        public void setIsAppendOnly(Boolean isAppendOnly) {
            this.isAppendOnly = isAppendOnly;
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getRecordType() {
            return TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){});
        }

        @Override
        public void emitDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
            consumeDataStream(dataStream);
        }



        @Override
        public TableSink<Tuple2<Boolean, Tuple2<String, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            MemoryUpsertSink memoryUpsertSink = new MemoryUpsertSink();
            memoryUpsertSink.setFieldNames(fieldNames);
            memoryUpsertSink.setFieldTypes(fieldTypes);
            memoryUpsertSink.setKeyFields(keyFields);
            memoryUpsertSink.setIsAppendOnly(isAppendOnly);

            return memoryUpsertSink;
        }

        @Override
        public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
            return dataStream.addSink(new DataSink()).setParallelism(1);
        }

        public TableSchema getSchema() {
            return schema;
        }

        public String[] getKeyFields() {
            return keyFields;
        }

        public boolean isAppendOnly() {
            return isAppendOnly;
        }

        @Override
        public String[] getFieldNames() {
            return this.schema.getFieldNames();
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return this.schema.getFieldTypes();
        }
    }

    private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, Long>>> {
        public DataSink() {
        }

        @Override
        public void invoke(Tuple2<Boolean, Tuple2<String, Long>> value, Context context) throws Exception {
            System.out.println("send message:" + value);
        }
    }
}
