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
public class UseCatalogCreateTable1 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String name = "hermes";
        String defaultDatabase = "smcjob_default";
        String hiveConfDir = "/Users/yyb/Downloads/hive-conf";
        String version = "2.1.1";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");

        String sql = "  CREATE TABLE yanfa_log1 (\n" +
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
                "     proctime AS PROCTIME()" +
                " ) WITH (\n" +
                "     'connector' = 'kafka-0.11',\n" +
                "     'topic' = 'x-log-yanfa_log',\n" +
                "     'properties.bootstrap.servers' = '172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092',\n" +
                "     'startup-mode' = 'latest-offset',\n" +
                "     'update-mode' = 'append',\n" +
                "     'format' = 'json'\n" +
                " )";

        tableEnv.executeSql(sql);

    }
}
