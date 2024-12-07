package com.datastreaming._deprecated;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;

public class SimpleProducer {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"); // Replace with your Kafka server(s)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas to acknowledge

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "my_topic"; // Replace with your topic name
        String message = "Hello, Kafka!";

        // Create a ProducerRecord to send to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message);

        // Send the record asynchronously
        producer.send(record, new Callback() {
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
        });

        // Ensure all messages are sent before closing
        producer.flush();

        // Close the producer
        producer.close();
    }

}
