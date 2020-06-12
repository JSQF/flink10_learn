package com.yyb.flink10.table.blink.stream.kafka;


import com.yyb.flink10.LoadClassByClassloader;
import com.yyb.flink10.commonEntity.Pi;
import com.yyb.flink10.util.GeneratorClassByASM;
import net.sf.cglib.core.ReflectUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
  * 注意 这里 涉及到了 ASM 动态产生 class  并加载的 内容，可以参考 https://blog.csdn.net/u010374412/article/details/106714721 博文
  * @Author yyb
  * @Description
  * @Date Create in 2020-06-10
  * @Time 09:32
  */
public class ReadDataFromKafkaConnectorJava {
  public static void main(String[] args) throws Exception {
    String packageName = "com.yyb.flink10.xxx.";
    String className = "Pi";
    byte[] byteOfClass = GeneratorClassByASM.geneClassMain(packageName, className);
    Class piCLass = ReflectUtils.defineClass(packageName + className, byteOfClass, ReadDataFromKafkaConnectorJava.class.getClassLoader());
    Class<?> xx = Class.forName(packageName + className);
    System.out.println(xx.newInstance());

    EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment flinkTableEnv = StreamTableEnvironment.create(env, settings);

//    env.registerType(pi);

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


//    DataStream<Pi> testDataStream = flinkTableEnv.toAppendStream(test, Pi.class);

//    Class.forName(pi.getName(), true, GeneratorClassByASM.cl).newInstance();
//    System.out.println(pi.getName());
    DataStream<Pi> testDataStream = flinkTableEnv.toAppendStream(test, piCLass);

    testDataStream.print().setParallelism(1);

    flinkTableEnv.execute("ReadDataFromKafkaConnector");
  }

}
