package com.datastreaming.sample_datastream_test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SampleDatastreamConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "sample-ds-group-log";
    private static final String TOPIC_IN = "sample-datastream-raw";
    private static final String TOPIC_OUT = "sample-datastream-es";

    private static final ObjectMapper objectMapper = new ObjectMapper();

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

    private static KafkaConsumer<String, String> buildConsumer() {
        // Kafka Consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // Replace with your Kafka server(s)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID); // Consumer group ID
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

        // Create consumer
        KafkaConsumer<String, String> consumer = buildConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_IN));

        try {
            while (true) {

                var records = consumer.poll(Duration.ofMillis(1000)); // Timeout in milliseconds

                for (ConsumerRecord<String, String> record : records) {

                    // System.out.printf("Consumed record with key: %s, value: %s, at offset %d%n",
                    // record.key(), record.value(), record.offset());

                    // Reformat records
                    JsonNode recordContent = objectMapper.readTree(record.value());
                    ObjectNode outputNode = objectMapper.createObjectNode();
                    outputNode.put("id", recordContent.has("id") ? recordContent.get("id").asInt() : 0);
                    outputNode.put("username",
                            recordContent.has("username") ? recordContent.get("username").asText() : "");
                    outputNode.put("message",
                            recordContent.has("message") ? recordContent.get("message").asText() : "");

                    // Serialize to JSON
                    String jsonValue = objectMapper.writeValueAsString(outputNode);

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, null, jsonValue);
                    producer.send(producerRecord, producerCallback);

                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            // Close producer and consumer
            consumer.close();
            producer.flush();
            producer.close();
        }

    }
}
