package com.datastreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AnimeDetailsProducer {

    private static final String TOPIC_OUT = "to-elastic-search";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final KafkaProducer<String, String> producer;

    static {
        // Kafka Producer configuration
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Kafka producer
        producer = new KafkaProducer<>(producerProps);
    }

    public static void sendToKafka(String message) {
        try {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, message);
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.println("Message sent to topic " + metadata.topic() +
                            " partition " + metadata.partition() +
                            " with offset " + metadata.offset());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        producer.close();
    }
}