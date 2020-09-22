package com.yyb.flink10.table.blink.stream.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.HashMap;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-21
 * @Time 16:42
 */
public class UseCatalogCreateTable {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String name = "myhive";
        String defaultDatabase = "test_zcy";
        String hiveConfDir = WriteData2HiveJavaReadFromkafkaTableSource.class.getResource("/").getFile();  //可以通过这一种方式设置 hiveConfDir，这样的话，开发与测试和生产环境可以保持一致
        String version = "2.1.1";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");

        LogicalType logicalType = new TimestampType(false, TimestampKind.PROCTIME, 3);
        AtomicDataType atomicDataType = new AtomicDataType(logicalType);
        Schema schema = new Schema();
        TableSchema tableSchema = TableSchema.builder()
                .field("order_id", DataTypes.STRING())
                .field("product_id", DataTypes.INT())
                .field("create_time", DataTypes.TIMESTAMP(3))
                .field("proctime", atomicDataType, "PROCTIME()")
                .build();

        HashMap<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "kafka-0.11");
        properties.put("topic", "flink_sql_test");
        properties.put("properties.bootstrap.servers",
                "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092");
        properties.put("scan.startup.mode", "latest-offset");
        properties.put("format", "json");
        //properties.put("update-mode", "append");

        ObjectPath tablePath = new ObjectPath("test_zcy", "kafka_order_yyb1");
        hive.createTable(tablePath, new CatalogTableImpl(
                        tableSchema,
                        properties,
                        "kafka table"
                ),
                false);

        String sql = "  CREATE TABLE xxxx (\n" +
                "     dt TIMESTAMP(3),\n" +
                "     conn_id STRING,\n" +
                "     sequence STRING,\n" +
                "     trace_id STRING,\n" +
                "     span_info STRING,\n" +
                "     service_id STRING,\n" +
                "     msg_id STRING,\n" +
                "     servicename STRING,\n" +
                "     ret_code STRING,\n" +
                "     duration STRING,\n" +
                "     req_body MAP<String,String>,\n" +
                "     res_body MAP<STRING,STRING>,\n" +
                "     extra_info MAP<STRING,STRING>,\n" +
                "     WATERMARK FOR dt AS dt - INTERVAL '60' SECOND,\n" +
                "     proctime AS PROCTIME()\n" +
                " ) WITH (\n" +
                "     'connector.type' = 'kafka',\n" +
                "     'connector.version' = '0.11',\n" +
                "     'connector.topic' = 'x-log-yanfa_log',\n" +
                "     'connector.properties.bootstrap.servers' = '******:9092',\n" +
                "     'connector.properties.zookeeper.connect' = '******:2181',\n" +
                "     'connector.startup-mode' = 'latest-offset',\n" +
                "     'update-mode' = 'append',\n" +
                "     'format.type' = 'json',\n" +
                "     'format.fail-on-missing-field' = 'true'\n" +
                " )";

        sql =   "CREATE TABLE kafka_order_yyb (\n" +
        "     order_id BIGINT,\n" +
                "     product_id INT,\n" +
                "     create_time TIMESTAMP(3),\n" +
                "     proctime AS PROCTIME()\n" +
                " ) WITH (\n" +
                "     'connector.type' = 'kafka',\n" +
                "     'connector.version' = '0.11',\n" +
                "     'connector.topic' = 'kafka_order',\n" +
                "     'connector.properties.bootstrap.servers' = '172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092',\n" +
                "     'connector.startup-mode' = 'latest-offset',\n" +
                "     'update-mode' = 'append',\n" +
                "     'format.type' = 'json',\n" +
                "     'format.fail-on-missing-field' = 'true'\n" +
                " )"
        ;

//        tableEnv.executeSql(sql);

//        env.execute("UseCatalogCreateTable");
    }
}
