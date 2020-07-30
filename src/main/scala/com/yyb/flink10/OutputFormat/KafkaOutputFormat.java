package com.yyb.flink10.OutputFormat;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kafka011.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-07-29
 * @Time 17:45
 */
public class KafkaOutputFormat extends RichOutputFormat<String> {
    private Properties properties;

    private FlinkKafkaProducer flinkKafkaProducer;
    public KafkaOutputFormat(Properties properties){
        this.properties = properties;
    }


    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        flinkKafkaProducer = new FlinkKafkaProducer<String, String>(properties);
    }

    @Override
    public void writeRecord(String record) throws IOException {
        ProducerRecord<String, String> recordP = new ProducerRecord<String, String>(this.properties.getProperty("topic"), record);
        flinkKafkaProducer.send(recordP);
    }

    @Override
    public void close() throws IOException {
        flinkKafkaProducer.flush();
        flinkKafkaProducer.close();
    }
}
