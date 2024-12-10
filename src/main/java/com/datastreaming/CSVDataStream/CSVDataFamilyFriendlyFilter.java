package com.datastreaming.CSVDataStream;

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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * This class reads messages from a Kafka topic, filters out anime that are not family-friendly,
 * and sends the filtered messages to another Kafka topic.
 */

public class CSVDataFamilyFriendlyFilter {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "family-friendly-group";
    private static final String TOPIC_IN = "sample-enriched-datastream";
    private static final String TOPIC_OUT = "es-anime-data";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Set<String> EXPLICIT_GENRES = new HashSet<>();

    static {
        EXPLICIT_GENRES.add("Hentai");
        EXPLICIT_GENRES.add("Ecchi");
        EXPLICIT_GENRES.add("Erotica");
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = buildConsumer();
        KafkaProducer<String, String> producer = buildProducer();
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    JsonNode jsonNode = objectMapper.readTree(record.value());
                    if (isFamilyFriendly(jsonNode)) {
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, record.value());
                        producer.send(producerRecord, (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.println("Sent filtered record to topic " + metadata.topic() + " partition " + metadata.partition() + " with offset " + metadata.offset());
                            }
                        });
                    }
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    /**
     * Creates and configures a Kafka consumer.
     *
     * @return KafkaConsumer object.
     */

    private static KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    /**
     * Creates and configures a Kafka producer.
     *
     * @return KafkaProducer object.
     */

    private static KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);  // Time before message delivery
        props.put(ProducerConfig.LINGER_MS_CONFIG, 200);  // Max time before sending message batch
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas to acknowledge
        return new KafkaProducer<>(props);
    }

    /**
     * Check if the anime is family-friendly.
     *
     * @param jsonNode
     * @return boolean
     */

    private static boolean isFamilyFriendly(JsonNode jsonNode) {
        JsonNode genresNode = jsonNode.get("genres");
        if (genresNode != null) {
            for (JsonNode genre : genresNode) {
                if (EXPLICIT_GENRES.contains(genre.get("name").asText())) {
                    return false;
                }
            }
        }
        return true;
    }
}

