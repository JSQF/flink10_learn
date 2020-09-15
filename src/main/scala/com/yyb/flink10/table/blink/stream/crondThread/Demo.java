package com.yyb.flink10.table.blink.stream.crondThread;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-15
 * @Time 14:00
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        MyThread myThread = new MyThread();
        myThread.setDaemon(true);
        myThread.start();



        Kafka kafka = new Kafka();
        kafka.version("0.11")
                .topic("eventsource_yyb_pi")
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
        ConnectTableDescriptor tableSource = tableEnv.connect(kafka)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema);
        tableSource.createTemporaryTable("test");
        String sql = "select * from test";

        Table test = tableEnv.from("test");
        test.printSchema();
        DataStream<Row> testDS = tableEnv.toAppendStream(test, Row.class);
        testDS.print().setParallelism(1);

        env.execute("Demo");
    }

    static class MyThread extends Thread{
        @Override
        public void run() {
            for(int i =0 ;i < 100; i++){
                System.out.println("mythreda :" + i);
                try {
                    Thread.sleep(800);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
