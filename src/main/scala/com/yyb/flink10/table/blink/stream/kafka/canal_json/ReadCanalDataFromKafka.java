package com.yyb.flink10.table.blink.stream.kafka.canal_json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.InputStream;
import java.util.Properties;

import static org.apache.flink.table.api.DataTypes.*;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-08
 * @Time 09:59
 */
public class ReadCanalDataFromKafka {
    public static void main(String[] args) throws Exception{
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

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

        final RowType SCHEMA = (RowType) ROW(
                FIELD("id", INT().notNull()),
                FIELD("name", STRING()),
                FIELD("description", STRING()),
                FIELD("weight", FLOAT())
        ).getLogicalType();
        CanalJsonDeserializationSchema canal_json_schema = new CanalJsonDeserializationSchema(
                SCHEMA,
                new RowDataTypeInfo(SCHEMA),
                true,
                TimestampFormat.SQL
        );

        FlinkKafkaConsumer011 kafkaSource = new FlinkKafkaConsumer011("eventsource_canal_yyb", canal_json_schema, properties);

        DataStreamSource kafkaSourceDS = env.addSource(kafkaSource);

//        kafkaSourceDS.print().setParallelism(1);

        Table canal_table = tableEnv.fromDataStream(kafkaSourceDS);
        tableEnv.createTemporaryView("canal_table", canal_table);

        DataStream<RowData> tsedDS = tableEnv.toAppendStream(canal_table, RowData.class);
//        tsedDS.print().setParallelism(1);
        tsedDS.map(new MapFunction<RowData, Row>() {
            @Override
            public Row map(RowData rowData) throws Exception {
                int length = rowData.getArity();
                RowKind operatorType = rowData.getRowKind();
                System.out.println("operatorType: "+ operatorType + ", length: " + length);
                return null;
            }
        });

        env.execute("ReadCanalDataFromKafka");

    }
}
