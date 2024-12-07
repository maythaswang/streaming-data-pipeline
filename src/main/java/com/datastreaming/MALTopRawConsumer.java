package com.datastreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MALAnimeDetailsConsumer {

    private static final String TOPIC_IN = "anime_details_topic";
    private static final String TOPIC_OUT = "anime_details_topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String MAL_API_URL_TEMPLATE = "https://api.myanimelist.net/v2/anime/%d?fields=id,title,rank,mean,genres,num_episodes,average_episode_duration,studios";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TIMEOUT = 5000; // 5 seconds
    private static final int MAX_RETRIES = 3; // Maximum number of retries

    public static void main(String[] args) {
        try {
            // Load properties from config file
            Properties configProps = new Properties();
            try (InputStream input = MALAnimeDetailsConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (input == null) {
                    System.out.println("Failed to load config.properties");
                    return;
                }
                configProps.load(input);
            }

            String clientId = configProps.getProperty("client_id");

            // Kafka Consumer configuration
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mal-anime-details-consumer-group");
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
                    if (records.isEmpty()) {
                        System.out.println("No messages found");
                    } else {
                        for (ConsumerRecord<String, String> record : records) {
                            boolean success = false;
                            int retries = 0;

                            while (!success && retries < MAX_RETRIES) {
                                try {
                                    int animeId = Integer.parseInt(record.value());
                                    String apiUrl = String.format(MAL_API_URL_TEMPLATE, animeId);
                                    URL url = new URL(apiUrl);
                                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                                    connection.setRequestMethod("GET");
                                    connection.setRequestProperty("X-MAL-CLIENT-ID", clientId);
                                    connection.setConnectTimeout(TIMEOUT);
                                    connection.setReadTimeout(TIMEOUT);

                                    int responseCode = connection.getResponseCode();
                                    if (responseCode == 504) {
                                        System.out.println("504 Gateway Timeout. Retrying...");
                                        retries++;
                                        continue;
                                    }

                                    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                                    String inputLine;
                                    StringBuilder content = new StringBuilder();
                                    while ((inputLine = in.readLine()) != null) {
                                        content.append(inputLine);
                                    }
                                    in.close();
                                    connection.disconnect();

                                    // Parse the API response
                                    JsonNode responseJson = objectMapper.readTree(content.toString());
                                    ObjectNode outputJson = objectMapper.createObjectNode();
                                    outputJson.put("id", getJsonNodeValue(responseJson, "id", 0));
                                    outputJson.put("title", getJsonNodeValue(responseJson, "title", ""));
                                    outputJson.put("rank", getJsonNodeValue(responseJson, "rank", 0));
                                    outputJson.put("mean", getJsonNodeValue(responseJson, "mean", 0.0));
                                    outputJson.put("num_episodes", getJsonNodeValue(responseJson, "num_episodes", 0));
                                    outputJson.put("average_episode_duration", getJsonNodeValue(responseJson, "average_episode_duration", ""));
                                    outputJson.set("genres", responseJson.get("genres"));
                                    outputJson.set("studios", responseJson.get("studios"));

                                    // Serialize the transformed JSON
                                    String transformedMessage = objectMapper.writeValueAsString(outputJson);

                                    // Send the data to the producer
                                    AnimeDetailsProducer.sendToKafka(transformedMessage);
                                    success = true;
                                } catch (SocketTimeoutException e) {
                                    System.out.println("Read timed out. Retrying...");
                                    retries++;
                                } catch (Exception e) {
                                    System.err.println("Failed to process record: " + record.value());
                                    e.printStackTrace();
                                    retries++;
                                    if (retries >= MAX_RETRIES) {
                                        System.err.println("Max retries reached. Resending record: " + record.value());
                                        producer.send(new ProducerRecord<>(TOPIC_OUT, record.value()));
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                // Close the consumer and producer when done
                consumer.close();
                producer.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static <T> T getJsonNodeValue(JsonNode node, String fieldName, T defaultValue) {
        JsonNode valueNode = node.get(fieldName);
        if (valueNode == null || valueNode.isNull()) {
            return defaultValue;
        }
        if (defaultValue instanceof Integer) {
            return (T) Integer.valueOf(valueNode.asInt());
        } else if (defaultValue instanceof Double) {
            return (T) Double.valueOf(valueNode.asDouble());
        } else if (defaultValue instanceof String) {
            return (T) valueNode.asText();
        }
        return defaultValue;
    }
}