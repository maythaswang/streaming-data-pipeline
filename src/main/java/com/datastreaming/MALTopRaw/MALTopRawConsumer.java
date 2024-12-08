package com.datastreaming.MALTopRaw;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * A Kafka consumer that reads the top anime IDs from a Kafka topic and sends
 * them to another Kafka topic.
 * This consumer is used to process the raw data from the MyAnimeList API.
 */

public class MALTopRawConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "raw-anime-consumer-group";
    private static final String TOPIC_IN = "top_anime_topic";
    private static final String TOPIC_OUT = "anime_details_topic";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final KafkaProducer<String, String> producer = buildProducer();
    private static final Callback producerCallback = buildProducerCallback();

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

    /**
     * @brief Reformat the incoming record and forward it to the next consumer
     * @param record
     */
    private static void forwardRecord(ConsumerRecord<String, String> record) {
        try {
            // Parse the incoming JSON
            JsonNode inputJson = objectMapper.readTree(record.value());
            JsonNode dataArray = inputJson.get("data");

            // Send anime IDs to the new Kafka topic
            for (JsonNode dataNode : dataArray) {
                JsonNode node = dataNode.get("node");
                int animeId = node.get("id").asInt();

                // Send the anime ID to the new Kafka topic
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT,
                        String.valueOf(animeId));
                producer.send(producerRecord, producerCallback);
            }
        } catch (Exception e) {
            System.err.println("Failed to process record: " + record.value());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = buildConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        // Forward cleaned record to the next consumer
        try {
            while (true) {
                // Poll for new messages from Kafka
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    forwardRecord(record);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            // Close the consumer and producer when done
            consumer.close();
            producer.close();
        }
    }
}