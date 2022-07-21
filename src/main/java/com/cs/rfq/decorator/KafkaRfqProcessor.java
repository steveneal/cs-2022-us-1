package com.cs.rfq.decorator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaRfqProcessor {
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "rfq-events";
    private static final String RFQ_STREAM_GROUP_ID = "rfq_stream-group_id";

    protected final JavaStreamingContext streamingContext;
    protected final SparkSession session;

    public KafkaRfqProcessor(SparkSession session, JavaStreamingContext context){
        this.streamingContext = context;
        this.session = session;
    }

    public JavaDStream<String> initRfqStream() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, RFQ_STREAM_GROUP_ID);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Collection<String> topics = Arrays.asList(TOPIC_NAME);

        JavaInputDStream<ConsumerRecord<String, String>> records =
                KafkaUtils.createDirectStream(
                 streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // extract value from ConsumerRecords
        JavaDStream<String> rfqStream = records.map(record -> record.value());

        return rfqStream;
    }

}

