package com.datastreaming._deprecated;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.*;

import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {

    private static final String TOPIC_IN = "my_topic";
    private static final String TOPIC_OUT = "to-elastic-search";

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

    private static KafkaProducer<String, String> buildProducer() {
        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"); // Replace with your Kafka server(s)
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

    public static void main(String[] args) {
        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = buildConsumer();
        // Subscribe to the Kafka topic
        String topic = TOPIC_IN;
        consumer.subscribe(Arrays.asList(topic));

        KafkaProducer<String, String> producer = buildProducer();
        Callback producerCallback = buildProducerCallback();
        // Poll for new messages from Kafka
        try {
            while (true) {
                // Polling the topic for new messages

                ObjectMapper objectMapper = new ObjectMapper();

                var records = consumer.poll(Duration.ofMillis(1000)); // Timeout in milliseconds
                for (ConsumerRecord<String, String> record : records) {
                    // Print the message received from the topic
                    System.out.printf("Consumed record with key: %s, value: %s, at offset %d%n",
                            record.key(), record.value(), record.offset());
                    SamplePayload samplePayload = new SamplePayload(record.value());

                    String jsonValue = objectMapper.writeValueAsString(samplePayload); // Convert the user object to
                                                                                       // JSON string

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, null, jsonValue); 
                    producer.send(producerRecord, producerCallback);
                    System.out.println(jsonValue);
                }
            }
        } catch (Exception e) {
            System.out.println("Error consuming messages: " + e.getMessage());
        } finally {
            // Close the consumer when done
            consumer.close();
            producer.close();
        }
    }
}