package com.datastreaming.CSVDataStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * This class reads a JSON object from a Kafka topic, processes it, and sends the processed JSON object to another Kafka topic.
 * The input JSON object is expected to have the following format:
 * {
 * "anime_id": <anime_id>,
 * "title": "<title>"
 * }
 * The output JSON object will have the same format.
 */

public class CSVDataStreamProcessor {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String APPLICATION_ID = "sample-ds-processor";
    private static final String TOPIC_IN = "sample-datastream-raw";
    private static final String TOPIC_OUT = "sample-datastream-es";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Creates the properties required for configuring the Kafka Streams application.
     *
     * @return Properties object containing the configuration.
     */
    private static Properties createStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    public static void main(String[] args) {
        Properties streamProperties = createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream(TOPIC_IN, Consumed.with(Serdes.String(), Serdes.String()));
        inputStream.mapValues(value -> processRecord(value)).to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamProperties);
        CountDownLatch latch = new CountDownLatch(1);

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Processes a single record from the input stream.
     *
     * @param value The JSON string from the input stream.
     * @return The processed JSON string.
     */
    private static String processRecord(String value) {
        try {
            JsonNode jsonNode = objectMapper.readTree(value);
            int animeId = jsonNode.get("anime_id").asInt();
            ObjectNode outputJson = objectMapper.createObjectNode();
            outputJson.put("anime_id", animeId);
            outputJson.put("title", jsonNode.get("title").asText());
            return objectMapper.writeValueAsString(outputJson);
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}