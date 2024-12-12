package com.datastreaming.CSVDataStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class reads messages from a Kafka topic, fetches data from the MyAnimeList API or Jikan API,
 * processes the data, and sends the processed messages to another Kafka topic.
 * The processed messages are filtered to exclude anime with explicit genres.
 * The Kafka topic that this class writes to will contain messages in the following format:
 * {
 * "id": <id>,
 * "title": "<title>",
 * "rank": <rank>,
 * "mean": <mean>,
 * "num_episodes": <num_episodes>,
 * "genres": [{"name": "<genre_name>"}],
 * "studios": [{"name": "<studio_name>"}]
 * }
 */

public class CSVKStreamProcessor {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String APPLICATION_ID = "csv-data-stream-processor";
    private static final String INPUT_TOPIC = "csv-raw";
    private static final String OUTPUT_TOPIC = "es-anime-data";
    private static final String MAL_API_URL_TEMPLATE = "https://api.myanimelist.net/v2/anime/%d?fields=id,title,rank,mean,genres,num_episodes,average_episode_duration,studios";
    private static final String JIKAN_API_URL_TEMPLATE = "https://api.jikan.moe/v4/anime/%d/full";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int TIMEOUT = 5000;
    private static final int MAL_RETRY_INTERVAL = 60000;
    private static final int JIKAN_INTERVAL = 800;
    private static final int MAX_RETRIES = 3;
    private static final Set<String> EXPLICIT_GENRES = new HashSet<>();

    static {
        EXPLICIT_GENRES.add("Hentai");
        EXPLICIT_GENRES.add("Ecchi");
        EXPLICIT_GENRES.add("Erotica");
    }

    public static void main(String[] args) {
        Properties props = getProperties();

        AtomicLong lastMalRetryTime = new AtomicLong();
        AtomicBoolean useMalApi = new AtomicBoolean(true);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));


        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("anime-state-store"),
                Serdes.String(),
                Serdes.String()
        ));

        KStream<String, String> processedStream = inputStream.transformValues(() -> new ValueTransformerWithKey<>() {
            private KeyValueStore<String, String> stateStore;

            @Override
            public void init(ProcessorContext context) {
                stateStore = context.getStateStore("anime-state-store");
            }

            @Override
            public String transform(String readOnlyKey, String value) {
                System.out.println("Received data: " + value);
                try {
                    JsonNode jsonNode = objectMapper.readTree(value);
                    int animeId = jsonNode.get("anime_id").asInt();
                    String clientId = loadClientId();
                    int retryCount = 0;

                    while (retryCount < MAX_RETRIES) {
                        try {
                            if (!useMalApi.get() && System.currentTimeMillis() - lastMalRetryTime.get() < MAL_RETRY_INTERVAL) {
                                Thread.sleep(JIKAN_INTERVAL);
                            } else {
                                useMalApi.set(true);
                            }

                            String apiUrl = useMalApi.get() ? String.format(MAL_API_URL_TEMPLATE, animeId) : String.format(JIKAN_API_URL_TEMPLATE, animeId);
                            JsonNode responseJson = fetchApiData(apiUrl, clientId, useMalApi.get());

                            ObjectNode outputJson = createOutputJson(responseJson, useMalApi.get());
                            String processedValue = objectMapper.writeValueAsString(outputJson);
                            System.out.println("Processed data: " + processedValue);
                            stateStore.put(readOnlyKey, processedValue);
                            return processedValue;
                        } catch (SocketTimeoutException e) {
                            useMalApi.set(false);
                            lastMalRetryTime.set(System.currentTimeMillis());
                        } catch (IOException e) {
                            retryCount++;
                        }
                    }

                    System.err.println("Max retries reached for anime ID: " + animeId);

                } catch (Exception e) {
                    System.err.println("Failed to process value: " + value);
                    e.printStackTrace();
                }
                return value;
            }

            @Override
            public void close() {
                // No-op
            }
        }, "anime-state-store");


        KStream<String, String> filteredStream = processedStream.filter((key, value) -> {
            System.out.println("Filtering data: " + value);
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                JsonNode genresNode = jsonNode.get("genres");
                if (genresNode != null) {
                    for (JsonNode genre : genresNode) {
                        if (EXPLICIT_GENRES.contains(genre.get("name").asText())) {
                            System.out.println("Filtered out data due to explicit genre: " + value);
                            return false;
                        }
                    }
                }
                System.out.println("Data passed filter: " + value);
                return true;
            } catch (Exception e) {
                System.err.println("Failed to filter value: " + value);
                e.printStackTrace();
                return false;
            }
        });

        filteredStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("Kafka Streams started.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            System.out.println("Kafka Streams stopped.");
        }));
    }

    /**
     * Create the Kafka Streams properties.
     *
     * @return Properties
     */
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/kafka-streams-tmp/kafka-streams");
//        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    /**
     * Load the client ID from the config.properties file.
     *
     * @return String
     */

    private static String loadClientId() {
        Properties configProps = new Properties();
        try (InputStream input = CSVKStreamProcessor.class.getClassLoader().getResourceAsStream("config.properties")) {
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
     * Fetch data from the MyAnimeList API or Jikan API.
     *
     * @param apiUrl    The API URL
     * @param clientId  The MAL client ID
     * @param useMalApi Whether to use the MAL API
     * @return JsonNode The API response
     * @throws IOException If an I/O error occurs
     */

    private static JsonNode fetchApiData(String apiUrl, String clientId, boolean useMalApi) throws IOException {
        System.out.println("Fetching data from API: " + apiUrl + " (Using MAL API: " + useMalApi + ")");
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

        System.out.println("Received API response: " + content);
        return objectMapper.readTree(content.toString());
    }

    /**
     * Create the output JSON object.
     *
     * @param responseJson
     * @param useMalApi
     * @return ObjectNode
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
}
