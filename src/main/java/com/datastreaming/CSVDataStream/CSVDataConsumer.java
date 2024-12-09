package com.datastreaming.CSVDataStream;

import com.datastreaming.AnimeDetails.AnimeDetailsConsumer;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CSVDataConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "sample-ds-group";
    private static final String TOPIC_IN = "sample-datastream-raw";
    private static final String TOPIC_OUT = "sample-datastream-es";
    private static final String MAL_API_URL_TEMPLATE = "https://api.myanimelist.net/v2/anime/%d?fields=id,title,rank,mean,genres,num_episodes,average_episode_duration,studios";
    private static final String JIKAN_API_URL_TEMPLATE = "https://api.jikan.moe/v4/anime/%d/full";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TIMEOUT = 5000;
    private static final int MAL_RETRY_INTERVAL = 60000;
    private static final int MAX_RETRIES = 3;

    private static KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = buildConsumer();
        KafkaProducer<String, String> producer = buildProducer();
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        boolean useMalApi = true;
        long lastMalRetryTime = 0;

        try {
            Properties configProps = new Properties();
            try (InputStream input = AnimeDetailsConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (input == null) {
                    System.out.println("Failed to load config.properties");
                    return;
                }
                configProps.load(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String clientId = configProps.getProperty("client_id");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    JsonNode jsonNode;
                    int animeId;
                    try {
                        jsonNode = objectMapper.readTree(record.value());
                        animeId = jsonNode.get("anime_id").asInt();
                    } catch (Exception e) {
                        System.err.println("Invalid anime ID: " + record.value());
                        continue; // Skip this record
                    }
                    boolean success = false;
                    int retryCount = 0;

                    while (!success && retryCount < MAX_RETRIES) {
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
                            if (responseCode == 429) {
                                System.out.println("429 Too Many Requests. Retrying...");
                                retryCount++;
                                Thread.sleep(1000); // Wait before retrying
                                continue;
                            } else if (responseCode == 504) {
                                System.out.println("504 Gateway Timeout. Switching API...");
                                useMalApi = false;
                                lastMalRetryTime = System.currentTimeMillis();
                                continue;
                            }

                            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                            StringBuilder content = new StringBuilder();
                            String inputLine;
                            while ((inputLine = in.readLine()) != null) {
                                content.append(inputLine);
                            }
                            in.close();
                            connection.disconnect();

                            JsonNode responseJson = objectMapper.readTree(content.toString());
                            if (responseJson == null) {
                                throw new NullPointerException("Response JSON is null");
                            }

                            ObjectNode outputJson = objectMapper.createObjectNode();
                            if (useMalApi) {
                                outputJson.put("id", responseJson.has("id") ? responseJson.get("id").asInt() : 0);
                                outputJson.put("title", responseJson.has("title") ? responseJson.get("title").asText() : "");
                                outputJson.put("rank", responseJson.has("rank") ? responseJson.get("rank").asInt() : 0);
                                outputJson.put("mean", responseJson.has("mean") ? responseJson.get("mean").asDouble() : 0.0);
                                outputJson.put("num_episodes", responseJson.has("num_episodes") ? responseJson.get("num_episodes").asInt() : 0);

                                ArrayNode genresArray = objectMapper.createArrayNode();
                                JsonNode genresNode = responseJson.get("genres");
                                if (genresNode != null) {
                                    for (JsonNode genre : genresNode) {
                                        ObjectNode genreNode = objectMapper.createObjectNode();
                                        genreNode.put("name", genre.get("name").asText());
                                        genresArray.add(genreNode);
                                    }
                                }
                                outputJson.set("genres", genresArray);

                                ArrayNode studiosArray = objectMapper.createArrayNode();
                                JsonNode studiosNode = responseJson.get("studios");
                                if (studiosNode != null) {
                                    for (JsonNode studio : studiosNode) {
                                        ObjectNode studioNode = objectMapper.createObjectNode();
                                        studioNode.put("name", studio.get("name").asText());
                                        studiosArray.add(studioNode);
                                    }
                                }
                                outputJson.set("studios", studiosArray);
                            } else {
                                responseJson = responseJson.get("data");
                                if (responseJson == null) {
                                    throw new NullPointerException("Response JSON data is null");
                                }
                                outputJson.put("id", responseJson.has("mal_id") ? responseJson.get("mal_id").asInt() : 0);
                                outputJson.put("title", responseJson.has("title") ? responseJson.get("title").asText() : "");
                                outputJson.put("rank", responseJson.has("rank") ? responseJson.get("rank").asInt() : 0);
                                outputJson.put("mean", responseJson.has("score") ? responseJson.get("score").asDouble() : 0.0);
                                outputJson.put("num_episodes", responseJson.has("episodes") ? responseJson.get("episodes").asInt() : 0);

                                ArrayNode genresArray = objectMapper.createArrayNode();
                                JsonNode genresNode = responseJson.get("genres");
                                if (genresNode != null) {
                                    for (JsonNode genre : genresNode) {
                                        ObjectNode genreNode = objectMapper.createObjectNode();
                                        genreNode.put("name", genre.get("name").asText());
                                        genresArray.add(genreNode);
                                    }
                                }
                                outputJson.set("genres", genresArray);

                                ArrayNode studiosArray = objectMapper.createArrayNode();
                                JsonNode studiosNode = responseJson.get("studios");
                                if (studiosNode != null) {
                                    for (JsonNode studio : studiosNode) {
                                        ObjectNode studioNode = objectMapper.createObjectNode();
                                        studioNode.put("name", studio.get("name").asText());
                                        studiosArray.add(studioNode);
                                    }
                                }
                                outputJson.set("studios", studiosArray);
                            }

                            String enrichedData = objectMapper.writeValueAsString(outputJson);

                            // Validate JSON before sending
                            try {
                                objectMapper.readTree(enrichedData);
                                System.out.println("Sending JSON data: " + enrichedData); // Print the JSON data
                                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_OUT, enrichedData);
                                producer.send(producerRecord, (metadata, exception) -> {
                                    if (exception != null) {
                                        exception.printStackTrace();
                                    } else {
                                        System.out.println("Sent enriched data to topic " + metadata.topic() + " partition " + metadata.partition() + " with offset " + metadata.offset());
                                    }
                                });
                                success = true;
                            } catch (Exception e) {
                                System.err.println("Invalid JSON data: " + enrichedData);
                            }

                            if (!useMalApi) {
                                Thread.sleep(800);
                            }
                        } catch (SocketTimeoutException e) {
                            System.out.println("Read timed out. Switching API to Jikan...");
                            useMalApi = false;
                            lastMalRetryTime = System.currentTimeMillis();
                        } catch (IOException e) {
                            if (e instanceof java.io.FileNotFoundException) {
                                System.err.println("File not found: " + e.getMessage());
                                break; // Skip this record
                            } else {
                                System.err.println("Failed to process record: " + record.value());
                                e.printStackTrace();
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to process record: " + record.value());
                            e.printStackTrace();
                        }
                    }

                    if (!success) {
                        System.err.println("Max retries reached for record: " + record.value());
                    }

                    if (!useMalApi && System.currentTimeMillis() - lastMalRetryTime >= MAL_RETRY_INTERVAL) {
                        useMalApi = true;
                    }
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}