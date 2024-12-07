package com.datastreaming.sample_datastream_test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

public class SampleDatastreamConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC_IN = "sample_datastream_raw";
    private static final String TOPIC_OUT = "sample_datastream_es";

    private static final StringBuilder stringBuilder = new StringBuilder();

    private static KafkaProducer<String, String> buildProducer() {
        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas to acknowledge

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static Callback buildProducerCallback() {
        Callback producerCallback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    // If there is an error
                    System.out.println("Error sending message: " + exception.getMessage());
                    if (exception instanceof TimeoutException) {
                        System.out.println("Timeout error occurred.");
                    }
                } else {
                    // Successfully sent message
                    System.out.println("Message sent to topic " + metadata.topic() +
                            " partition " + metadata.partition() +
                            " with offset " + metadata.offset());
                }
            }
        };
        return producerCallback;
    }

    private static KafkaConsumer<String, String> buildConsumer() {
        // Kafka Consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"); // Replace with your Kafka server(s)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1"); // Consumer group ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start consuming from the earliest message

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public static void main(String[] args) {
        // Create producer and producerCallback
        KafkaProducer<String, String> producer = buildProducer();
        Callback producerCallback = buildProducerCallback();

        // Send payload
        // for (int i = 0; i < MAX_ITEM; i++) {
        // ProducerRecord<String, String> producerRecord = new
        // ProducerRecord<>(TOPIC_OUT, null, buildSampleMessage());
        // producer.send(producerRecord, producerCallback);
        // }

        // Close producer
        producer.flush();
        producer.close();
    }
}
