package com.datastreaming.sample_datastream_test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;

public class SampleDatastreamProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC_OUT = "sample-datastream-raw";
    private static final int MAX_ITEM = 10_000_000;

    private static final StringBuilder stringBuilder = new StringBuilder();

    private static KafkaProducer<String, String> buildProducer() {
        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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

    private static String buildSampleMessage() {
        stringBuilder.append("{");
        stringBuilder.append("\"username\":");
        stringBuilder.append("\"FOO\""); // Randomize name here
        stringBuilder.append(",\"id\":");
        stringBuilder.append("12345"); // Randomize id here
        stringBuilder.append(",\"message\":");
        stringBuilder.append("\"HELLO FROM KAFKA!\"");
        stringBuilder.append("}");
        String out = stringBuilder.toString();
        stringBuilder.setLength(0);
        return out;
    }

    public static void main(String[] args) {
        // Create producer and producerCallback
        KafkaProducer<String, String> producer = buildProducer();
        Callback producerCallback = buildProducerCallback();

        // Send payload
        for (int i = 0; i < MAX_ITEM; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, null, buildSampleMessage());
            producer.send(producerRecord, producerCallback);
        }

        // Close producer
        producer.flush();
        producer.close();
    }

}
