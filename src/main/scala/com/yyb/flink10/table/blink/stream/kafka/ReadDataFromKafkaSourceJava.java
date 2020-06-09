package com.yyb.flink10.table.blink.stream.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-09
 * @Time 22:46
 */
public class ReadDataFromKafkaSourceJava {
    public static void main(String[] args){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment flinkTableEnv = StreamTableEnvironment.create(env, settings);
        BasicTypeInfo<String> field1 = BasicTypeInfo.STRING_TYPE_INFO;
        BasicTypeInfo<String> field2 = BasicTypeInfo.STRING_TYPE_INFO;
        TableSchema schema = new TableSchema(new String[]{"field1", "field2"}, new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO});
        String topic = "topic";
        Properties properties = new Properties();
        DeserializationSchema deserializationSchema = new SimpleStringSchema();
        Kafka010TableSource kafkaSource = new Kafka010TableSource(schema, topic, properties, deserializationSchema);
    }
}
