package com.datastreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * A Kafka consumer that reads the top anime IDs from a Kafka topic and sends them to another Kafka topic.
 * This consumer is used to process the raw data from the MyAnimeList API.
 */

public class MALTopRawConsumer {

    private static final String TOPIC_IN = "top_anime_topic";
    private static final String TOPIC_OUT = "anime_details_topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        // Kafka Consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-anime-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        // Kafka Producer configuration
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        try {
            while (true) {
                // Poll for new messages from Kafka
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Parse the incoming JSON
                        JsonNode inputJson = objectMapper.readTree(record.value());
                        JsonNode dataArray = inputJson.get("data");

                        // Send anime IDs to the new Kafka topic
                        for (JsonNode dataNode : dataArray) {
                            JsonNode node = dataNode.get("node");
                            int animeId = node.get("id").asInt();

                            // Send the anime ID to the new Kafka topic
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, String.valueOf(animeId));
                            producer.send(producerRecord, (metadata, exception) -> {
                                if (exception != null) {
                                    System.out.println("Error sending message: " + exception.getMessage());
                                } else {
                                    System.out.println("Message sent to topic " + metadata.topic() +
                                            " partition " + metadata.partition() +
                                            " with offset " + metadata.offset());
                                }
                            });
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to process record: " + record.value());
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            // Close the consumer and producer when done
            consumer.close();
            producer.close();
        }
    }
}