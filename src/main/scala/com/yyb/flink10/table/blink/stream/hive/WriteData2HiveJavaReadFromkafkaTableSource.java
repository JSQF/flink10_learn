package com.yyb.flink10.table.blink.stream.hive;

import com.yyb.flink10.commonEntity.Pi;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.descriptors.Schema;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;


/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-07-07
 * @Time 14:28
 */
public class WriteData2HiveJavaReadFromkafkaTableSource {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Schema schema = new Schema();
        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("time", DataTypes.STRING())
                .build();
        schema.schema(tableSchema);
        Properties prop = new Properties();
        prop.put("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181");
        prop.put("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092");
        prop.put("group.id", "yyb_dev1");

        TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        String[] fields = new String[]{"id", "time"};
        RowTypeInfo rowTypeINfo = new RowTypeInfo(types, fields);
        JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema.Builder(rowTypeINfo).build();
//        Kafka010TableSource kafka = new Kafka010TableSource(tableSchema, "eventsource_yhj", prop, jsonRowDeserializationSchema);
        //指定 从 kafka 的 earliest 开始消费
        Kafka010TableSource kafka = new Kafka010TableSource(tableSchema, Optional.empty(), Collections.emptyList(), Optional.empty(),"eventsource_yhj", prop, jsonRowDeserializationSchema
        , StartupMode.EARLIEST, Collections.emptyMap(), 0);

        Table kafkaSource = tableEnv.fromTableSource(kafka);

        tableEnv.createTemporaryView("default_catalog.kafkaSource", kafkaSource);

        String sql ="select * from default_catalog.kafkaSource";
        tableEnv.sqlQuery(sql).printSchema();

        String name = "myhive";
        String defaultDatabase = "test";
        String hiveConfDir = WriteData2HiveJavaReadFromkafkaTableSource.class.getResource("/").getFile();  //可以通过这一种方式设置 hiveConfDir，这样的话，开发与测试和生产环境可以保持一致

//    val version = "2.3.6"
        String version = "1.1.0";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");

        sql = "insert into myhive.test.a select * from default_catalog.kafkaSource";
        tableEnv.sqlUpdate(sql);

        DataStream<Pi> kafkaSourceDataStream = tableEnv.toAppendStream(kafkaSource, Pi.class);
         kafkaSourceDataStream.print().setParallelism(1);
        tableEnv.execute("WriteData2Hive");
    }
}
