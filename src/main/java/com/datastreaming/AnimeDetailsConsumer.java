package com.datastreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

public class AnimeDetailsConsumer {

    private static final String TOPIC_IN = "anime_details_topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String MAL_API_URL_TEMPLATE = "https://api.myanimelist.net/v2/anime/%d?fields=id,title,rank,mean,genres,num_episodes,average_episode_duration,studios";
    private static final String JIKAN_API_URL_TEMPLATE = "https://api.jikan.moe/v4/anime/%d/full";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TIMEOUT = 5000;
    private static final int MAL_RETRY_INTERVAL = 60000;

    public static void main(String[] args) {
        try {
            // Load properties from config file
            Properties configProps = new Properties();
            try (InputStream input = AnimeDetailsConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {
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
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "anime-details-consumer-group");
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

            boolean useMalApi = true;
            long lastMalRetryTime = 0;

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    if (records.isEmpty()) {
                        System.out.println("No messages found");
                    } else {
                        for (ConsumerRecord<String, String> record : records) {
                            boolean success = false;
                            int animeId = Integer.parseInt(record.value());

                            while (!success) {
                                try {
                                    String apiUrl;
                                    if (useMalApi) {
                                        apiUrl = String.format(MAL_API_URL_TEMPLATE, animeId);
                                    } else {
                                        apiUrl = String.format(JIKAN_API_URL_TEMPLATE, animeId);
                                    }

                                    URL url = new URL(apiUrl);
                                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                                    connection.setRequestMethod("GET");
                                    if (useMalApi) {
                                        connection.setRequestProperty("X-MAL-CLIENT-ID", clientId);
                                    }
                                    connection.setConnectTimeout(TIMEOUT);
                                    connection.setReadTimeout(TIMEOUT);

                                    int responseCode = connection.getResponseCode();
                                    if (responseCode == 504) {
                                        System.out.println("504 Gateway Timeout. Switching API...");
                                        useMalApi = false;
                                        lastMalRetryTime = System.currentTimeMillis();
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

                                    JsonNode responseJson = objectMapper.readTree(content.toString());
                                    if (responseJson == null) {
                                        throw new NullPointerException("Response JSON is null");
                                    }
                                    if (useMalApi) {
                                        // MAL API response
                                        ObjectNode outputJson = objectMapper.createObjectNode();
                                        outputJson.put("id", responseJson.has("id") ? responseJson.get("id").asInt() : 0);
                                        outputJson.put("title", responseJson.has("title") ? responseJson.get("title").asText() : "");
                                        outputJson.put("rank", responseJson.has("rank") ? responseJson.get("rank").asInt() : 0);
                                        outputJson.put("mean", responseJson.has("mean") ? responseJson.get("mean").asDouble() : 0.0);
                                        outputJson.put("num_episodes", responseJson.has("num_episodes") ? responseJson.get("num_episodes").asInt() : 0);

                                        // Filter out unwanted fields from genres
                                        ArrayNode genresArray = objectMapper.createArrayNode();
                                        for (JsonNode genre : responseJson.get("genres")) {
                                            ObjectNode genreNode = objectMapper.createObjectNode();
                                            genreNode.put("name", genre.get("name").asText());
                                            genresArray.add(genreNode);
                                        }
                                        outputJson.set("genres", genresArray);

                                        // Filter out unwanted fields from studios
                                        ArrayNode studiosArray = objectMapper.createArrayNode();
                                        for (JsonNode studio : responseJson.get("studios")) {
                                            ObjectNode studioNode = objectMapper.createObjectNode();
                                            studioNode.put("name", studio.get("name").asText());
                                            studiosArray.add(studioNode);
                                        }
                                        outputJson.set("studios", studiosArray);

                                        // Serialize the transformed JSON
                                        String transformedMessage = objectMapper.writeValueAsString(outputJson);

                                        // Send the data to the producer
                                        AnimeDetailsProducer.sendToKafka(transformedMessage);
                                    } else {
                                        // Jikan API response
                                        responseJson = responseJson.get("data");
                                        if (responseJson == null) {
                                            throw new NullPointerException("Response JSON data is null");
                                        }
                                        ObjectNode outputJson = objectMapper.createObjectNode();
                                        outputJson.put("id", responseJson.has("mal_id") ? responseJson.get("mal_id").asInt() : 0);
                                        outputJson.put("title", responseJson.has("title") ? responseJson.get("title").asText() : "");
                                        outputJson.put("rank", responseJson.has("rank") ? responseJson.get("rank").asInt() : 0);
                                        outputJson.put("mean", responseJson.has("score") ? responseJson.get("score").asDouble() : 0.0);
                                        outputJson.put("num_episodes", responseJson.has("episodes") ? responseJson.get("episodes").asInt() : 0);

                                        // Filter out unwanted fields from genres
                                        ArrayNode genresArray = objectMapper.createArrayNode();
                                        for (JsonNode genre : responseJson.get("genres")) {
                                            ObjectNode genreNode = objectMapper.createObjectNode();
                                            genreNode.put("name", genre.get("name").asText());
                                            genresArray.add(genreNode);
                                        }
                                        outputJson.set("genres", genresArray);

                                        // Filter out unwanted fields from studios
                                        ArrayNode studiosArray = objectMapper.createArrayNode();
                                        for (JsonNode studio : responseJson.get("studios")) {
                                            ObjectNode studioNode = objectMapper.createObjectNode();
                                            studioNode.put("name", studio.get("name").asText());
                                            studiosArray.add(studioNode);
                                        }
                                        outputJson.set("studios", studiosArray);

                                        // Serialize the transformed JSON
                                        String transformedMessage = objectMapper.writeValueAsString(outputJson);

                                        // Send the data to the producer
                                        AnimeDetailsProducer.sendToKafka(transformedMessage);
                                    }
                                    success = true;
                                    if (!useMalApi) {
                                        Thread.sleep(800);
                                    }
                                } catch (SocketTimeoutException e) {
                                    System.out.println("Read timed out. Switching API to Jikan...");
                                    useMalApi = false;
                                    lastMalRetryTime = System.currentTimeMillis();
                                } catch (Exception e) {
                                    System.err.println("Failed to process record: " + record.value());
                                    e.printStackTrace();
                                }
                            }

                            // Check if it's time to retry the MAL API
                            if (!useMalApi && System.currentTimeMillis() - lastMalRetryTime >= MAL_RETRY_INTERVAL) {
                                useMalApi = true;
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
}