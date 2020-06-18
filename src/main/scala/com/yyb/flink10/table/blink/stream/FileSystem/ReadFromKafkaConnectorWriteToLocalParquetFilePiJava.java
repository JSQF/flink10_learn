package com.yyb.flink10.table.blink.stream.FileSystem;

import com.yyb.flink10.commonEntity.Pi;
import org.apache.avro.JsonProperties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import java.util.ArrayList;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-16
 * @Time 10:24
 */
public class ReadFromKafkaConnectorWriteToLocalParquetFilePiJava {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings setttings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment flinkTableEnv = StreamTableEnvironment.create(env, setttings);

        env.enableCheckpointing(20);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Kafka kafka = new Kafka();
        kafka.version("0.11")
                .topic("eventsource_yhj")
                .property("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181")
                .property("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092")
                .property("group.id", "yyb_dev")
                .startFromEarliest();

        Schema schema = new Schema();
        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("time", DataTypes.STRING())
                .build();
        schema.schema(tableSchema);
        ConnectTableDescriptor tableSource = flinkTableEnv.connect(kafka)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema);
        tableSource.createTemporaryTable("test");
        String sql = "select * from test";

        Table test = flinkTableEnv.from("test");
        test.printSchema();


        //transfor 2 dataStream
        DataStream<?> testDataStream = flinkTableEnv.toAppendStream(test, Pi.class); //使用 Class 的方式

        String fileSinkPath = "./xxx.text/rs6/";

        StreamingFileSink parquetSink = StreamingFileSink.
                forBulkFormat(new Path(fileSinkPath),
                        ParquetAvroWriters.forReflectRecord(Pi.class))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        testDataStream.addSink(parquetSink).setParallelism(1);

        flinkTableEnv.execute("ReadFromKafkaConnectorWriteToLocalFileJava");


    }
}
