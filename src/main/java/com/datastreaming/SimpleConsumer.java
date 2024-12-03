package com.datastreaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {

    private static final String TOPIC_IN = "my_topic";

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
        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = buildConsumer();

        // Subscribe to the Kafka topic
        String topic = TOPIC_IN;
        consumer.subscribe(Arrays.asList(topic));

        // Poll for new messages from Kafka
        try {
            while (true) {
                // Polling the topic for new messages
                var records = consumer.poll(Duration.ofMillis(1000)); // Timeout in milliseconds
                for (ConsumerRecord<String, String> record : records) {
                    // Print the message received from the topic
                    System.out.printf("Consumed record with key: %s, value: %s, at offset %d%n",
                            record.key(), record.value(), record.offset());
                }
            }
        } catch (Exception e) {
            System.out.println("Error consuming messages: " + e.getMessage());
        } finally {
            // Close the consumer when done
            consumer.close();
        }
    }
}