package com.kyle.spark230.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyProducer {


    public static void main(String[] args) throws IOException, InterruptedException {

        Properties properties = new Properties();
        InputStream in = MyProducer.class.getClassLoader().getResourceAsStream("kafka_producer.properties");
        properties.load(in);

        String[] girls = new String[]{"姚慧莹", "刘向前", "周  新", "杨柳"};
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = properties.getProperty("producer.topic");
        int n = 0;
        int key = 100107001;
        while (true){
            String value = "kyle_" + key;
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, String.valueOf(key), value);
            producer.send(producerRecord);
            if (n > 20){
                break;
            }
            n++;
            key++;
            Thread.sleep(1000);
        }
        producer.close();
    }


}
