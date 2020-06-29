package com.yyb.flink10.table.blink.stream.FileSystem;

import com.yyb.flink10.commonEntity.Pi;
import com.yyb.flink10.util.ParquetAvroWritersSelf;
import flink.api.java.Tuple2;
import org.apache.avro.JsonProperties;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.codehaus.jackson.JsonNode;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-16
 * @Time 10:24
 */
public class ReadFromKafkaConnectorWriteToLocalParquetFileJava {
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
                .withSchema(schema)
                ;
        tableSource.createTemporaryTable("test");
        String sql = "select * from test";

        Table test = flinkTableEnv.from("test");
        test.printSchema();



        //transfor 2 dataStream
//        TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(GenericData.Record.class, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(Tuple2.class, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        DataStream testDataStream = flinkTableEnv.toAppendStream(test, tupleTypeInfo);

        testDataStream.print().setParallelism(1);


        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<org.apache.avro.Schema.Field>();
        fields.add(new org.apache.avro.Schema.Field("id", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), "id", JsonProperties.NULL_VALUE));
        fields.add(new org.apache.avro.Schema.Field("time", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), "time", JsonProperties.NULL_VALUE));
        org.apache.avro.Schema parquetSinkSchema = org.apache.avro.Schema.createRecord("pi", "flinkParquetSink", "flink.parquet", true, fields);
        String fileSinkPath = "./xxx.text/rs6/";


        GenericRecordAvroTypeInfo genericRecordAvroTypeInfo1 = new GenericRecordAvroTypeInfo(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        GenericRecordAvroTypeInfo genericRecordAvroTypeInfo2 = new GenericRecordAvroTypeInfo(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        TupleTypeInfo tupleTypeInfo1 = new TupleTypeInfo(genericRecordAvroTypeInfo1, genericRecordAvroTypeInfo2);

        DataStream testDataStream1 = flinkTableEnv.toAppendStream(test, tupleTypeInfo1);

        testDataStream1.print().setParallelism(1);


        StreamingFileSink<GenericRecord> parquetSink = StreamingFileSink.
                forBulkFormat(new Path(fileSinkPath),
                        ParquetAvroWriters.forGenericRecord(parquetSinkSchema))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        testDataStream1.addSink(parquetSink).setParallelism(1);
        flinkTableEnv.execute("ReadFromKafkaConnectorWriteToLocalFileJava");


    }
}
