package com.cs.rfq.decorator.publishers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public Producer(){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost: 9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ProducerRecord record = new ProducerRecord("tirades-topic", "name", "selftuts");

        KafkaProducer producer = new KafkaProducer(properties);

        producer.send(record);

        producer.close();


    }
}
