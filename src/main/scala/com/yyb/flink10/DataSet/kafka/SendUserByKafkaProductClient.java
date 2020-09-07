package com.yyb.flink10.DataSet.kafka;

import com.alibaba.fastjson.JSON;
import com.yyb.flink10.commonEntity.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-04
 * @Time 17:48
 */
public class SendUserByKafkaProductClient {
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
        String topic = "eventsource_order_yyb";

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        User user = new User();

        String message = JSON.toJSONString(user);

        producer.send(new ProducerRecord<String, String>(topic, message)).get();
    }
}
