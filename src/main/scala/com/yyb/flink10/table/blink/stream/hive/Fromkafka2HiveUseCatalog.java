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
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.descriptors.Schema;

import java.util.*;

/**
 * @Author yyb
 * @Description 经过多次尝试，目前 flink 不支持 table insert hive table
 * @Date Create in 2020-07-07
 * @Time 16:22
 */
public class Fromkafka2HiveUseCatalog {
    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "center");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String name = "myhive";
        String defaultDatabase = "test";
        String hiveConfDir = WriteData2HiveJavaReadFromkafkaTableSource.class.getResource("/").getFile();  //可以通过这一种方式设置 hiveConfDir，这样的话，开发与测试和生产环境可以保持一致
        String version = "2.1.1";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");

        /**
         * kafka start
         */
        Schema schema = new Schema();
        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("time", DataTypes.STRING())
                .build();
        schema.schema(tableSchema);
        Properties prop = new Properties();
        prop.put("zookeeper.connect", "172.16.10.16:2181,172.16.10.17:2181,172.16.10.18:2181");
        prop.put("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092");
        prop.put("group.id", "yyb_dev");

        TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        String[] fields = new String[]{"id", "time"};
        RowTypeInfo rowTypeINfo = new RowTypeInfo(types, fields);
        JsonRowDeserializationSchema jsonRowDeserializationSchema = new JsonRowDeserializationSchema.Builder(rowTypeINfo).build();
//        Kafka010TableSource kafka = new Kafka010TableSource(tableSchema, "eventsource_yhj", prop, jsonRowDeserializationSchema);
        //指定 从 kafka 的 earliest 开始消费
        Kafka010TableSource kafka = new Kafka010TableSource(
                tableSchema,
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty(),
                "eventsource_yhj",
                prop,
                jsonRowDeserializationSchema,
                StartupMode.EARLIEST,
                Collections.emptyMap(),
                0
        );

        Table kafkaTable = tableEnv.fromTableSource(kafka);

        tableEnv.createTemporaryView("kafkaTable", kafkaTable);

        /**
         * kafka end
         */


//        HashMap<String, String> map = new HashMap<String, String>();
//        map.put("k1", "v1");
//        hive.createDatabase("mydb", new CatalogDatabaseImpl(map, "test"),true);

//        hive.createTable(
//                new ObjectPath("myhive", "mytable"),
//                 ConnectorCatalogTable.source(kafka, false),
//                false
//                );
//
//        String sql = "insert into myhive.test.a select * from mydb.mytable";
//
//        tableEnv.sqlUpdate(sql);


        List<String> dbs = hive.listDatabases();
        for(String db : dbs){
            System.out.println(db);
        }

        System.out.println("------------------");

        List<String> tbs = hive.listTables("test");
        for(String tb : tbs){
            System.out.println(tb);
        }

        boolean xx = hive.tableExists(new ObjectPath("test", "a"));
        System.out.println(xx + " cvb--------------");
        Table sink = tableEnv.from("test.a");
        sink.printSchema();


//        tableEnv.insertInto("kafkaTable", sink);
        kafkaTable.insertInto("test.a");

        DataStream<Pi> kafkaSourceDataStream = tableEnv.toAppendStream(kafkaTable, Pi.class);
        kafkaSourceDataStream.print().setParallelism(1);
        tableEnv.execute("Fromkafka2HiveUseCatalog");





    }
}
