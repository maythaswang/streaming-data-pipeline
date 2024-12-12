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

/**
 * This class consumes JSON data from a Kafka topic and uses the MyAnimeList API and Jikan API for extra information.
 * The JSON data is enriched with additional information and sent to another Kafka topic.
 * The new JSON object has the following format:
 * {
 * "id": <id>,
 * "title": "<title>",
 * "rank": <rank>,
 * "mean": <mean>,
 * "num_episodes": <num_episodes>,
 * "genres": [{"name": "<genre_name>"}],
 * "studios": [{"name": "<studio_name>"}]
 * }
 * The MyAnimeList API is used first, and if it fails, the Jikan API is used as a fallback.
 * The CSV file is outdated so a retry mechanism is implemented to verify if the anime is still available.
 */

public class CSVDataConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "csv-consumer-group";
    private static final String TOPIC_IN = "csv-raw";
    private static final String TOPIC_OUT = "csv-enriched-datastream";
    private static final String MAL_API_URL_TEMPLATE = "https://api.myanimelist.net/v2/anime/%d?fields=id,title,rank,mean,genres,num_episodes,average_episode_duration,studios";
    private static final String JIKAN_API_URL_TEMPLATE = "https://api.jikan.moe/v4/anime/%d/full";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TIMEOUT = 5000;
    private static final int MAL_RETRY_INTERVAL = 60000;
    private static final int JIKAN_INTERVAL = 800;
    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = buildConsumer();
        KafkaProducer<String, String> producer = buildProducer();
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        boolean useMalApi = true;
        long lastMalRetryTime = 0;

        try {
            String clientId = loadClientId();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    useMalApi = processRecord(record, producer, clientId, useMalApi, lastMalRetryTime);
                    if (!useMalApi) {
                        Thread.sleep(JIKAN_INTERVAL);
                    }
//                    consumer.commitSync();
                }
                if (!useMalApi && System.currentTimeMillis() - lastMalRetryTime >= MAL_RETRY_INTERVAL) {
                    useMalApi = true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);  // Time before message delivery
        props.put(ProducerConfig.LINGER_MS_CONFIG, 200);  // Max time before sending message batch
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas to acknowledge
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
        return new KafkaProducer<>(props);
    }

    /**
     * Loads the client ID from the config.properties file.
     *
     * @return The client ID.
     */

    private static String loadClientId() {
        Properties configProps = new Properties();
        try (InputStream input = AnimeDetailsConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Failed to load config.properties");
            }
            configProps.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return configProps.getProperty("client_id");
    }

    /**
     * Processes a single record from the input stream.
     *
     * @param record           The Kafka ConsumerRecord object.
     * @param producer         The Kafka Producer object.
     * @param clientId         The MyAnimeList API client ID.
     * @param useMalApi        Flag to indicate if the MyAnimeList API should be used.
     * @param lastMalRetryTime The timestamp of the last MyAnimeList API retry.
     * @return The updated flag to indicate if the MyAnimeList API should be used.
     */

    private static boolean processRecord(ConsumerRecord<String, String> record, KafkaProducer<String, String> producer, String clientId, boolean useMalApi, long lastMalRetryTime) {
        JsonNode jsonNode;
        int animeId;
        try {
            jsonNode = objectMapper.readTree(record.value());
            animeId = jsonNode.get("anime_id").asInt();
        } catch (Exception e) {
            System.err.println("Invalid anime ID: " + record.value());
            return useMalApi;
        }

        boolean success = false;
        int retryCount = 0;

        while (!success && retryCount < MAX_RETRIES) {
            try {
                String apiUrl = useMalApi ? String.format(MAL_API_URL_TEMPLATE, animeId) : String.format(JIKAN_API_URL_TEMPLATE, animeId);
                JsonNode responseJson = fetchApiData(apiUrl, clientId, useMalApi);

                ObjectNode outputJson = createOutputJson(responseJson, useMalApi);
                String enrichedData = objectMapper.writeValueAsString(outputJson);

                validateAndSendData(producer, enrichedData);
                success = true;
            } catch (SocketTimeoutException e) {
                System.out.println("Read timed out. Switching API to Jikan...");
                useMalApi = false;
                lastMalRetryTime = System.currentTimeMillis();
            } catch (IOException e) {
                handleIOException(e, record);
                retryCount++;
            } catch (Exception e) {
                System.err.println("Failed to process record: " + record.value());
                e.printStackTrace();
            }
        }

        if (!success) {
            System.err.println("Max retries reached for record: " + record.value());
        }
        return useMalApi;
    }

    /**
     * Fetches data from the MyAnimeList or Jikan API.
     *
     * @param apiUrl    The API URL.
     * @param clientId  The MyAnimeList API client ID.
     * @param useMalApi Flag to indicate if the MyAnimeList API should be used.
     * @return The JSON response from the API.
     * @throws IOException If an error occurs during the API request.
     */

    private static JsonNode fetchApiData(String apiUrl, String clientId, boolean useMalApi) throws IOException {
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
            throw new IOException("429 Too Many Requests");
        } else if (responseCode == 504) {
            throw new IOException("504 Gateway Timeout");
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        StringBuilder content = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        connection.disconnect();

        return objectMapper.readTree(content.toString());
    }

    /**
     * Creates the enriched JSON output object.
     *
     * @param responseJson The JSON response from the API.
     * @param useMalApi    Flag to indicate if the MyAnimeList API was used.
     * @return The enriched JSON output object.
     */

    private static ObjectNode createOutputJson(JsonNode responseJson, boolean useMalApi) {
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
        return outputJson;
    }

    /**
     * Validates the enriched JSON data and sends it to the output Kafka topic.
     *
     * @param producer     The Kafka Producer object.
     * @param enrichedData The enriched JSON data.
     * @throws IOException If the JSON data is invalid.
     */

    private static void validateAndSendData(KafkaProducer<String, String> producer, String enrichedData) throws IOException {
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
        } catch (Exception e) {
            throw new IOException("Invalid JSON data: " + enrichedData, e);
        }
    }

    /**
     * Handles IOExceptions that occur during processing.
     *
     * @param e      The IOException object.
     * @param record The Kafka ConsumerRecord object.
     */

    private static void handleIOException(IOException e, ConsumerRecord<String, String> record) {
        if (e instanceof java.io.FileNotFoundException) {
            System.err.println("File not found: " + e.getMessage());
        } else {
            System.err.println("Failed to process record: " + record.value());
            e.printStackTrace();
        }
    }
}