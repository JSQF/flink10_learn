package com.yyb.flink10.DataSet.kafka;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-04
 * @Time 17:48
 */
public class SendCanalJsonDataByKafkaProductClient {
    public static void main(String[] args) throws Exception{
        InputStream in_env  = ClassLoader.getSystemResourceAsStream("env.properties");
        Properties prop = new Properties();
        prop.load(in_env);



        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", prop.getProperty("bootstrap.servers"));
        properties.setProperty("zookeeper.connect", prop.getProperty("zookeeper.connect"));
        properties.setProperty("group.id", "test");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        String topic = "eventsource_canal_yyb";

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        List<String> lines = readLines("canal-data.txt");
        for (String line : lines) {
            producer.send(new ProducerRecord<String, String>(topic, line)).get();
        }

    }

    private static List<String> readLines(String resource) throws IOException {
        final URL url = SendCanalJsonDataByKafkaProductClient.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }
}
