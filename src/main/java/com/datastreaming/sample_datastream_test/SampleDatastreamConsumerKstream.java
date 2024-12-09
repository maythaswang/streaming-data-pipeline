package com.datastreaming.sample_datastream_test;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KeyValue;

import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.common.serialization.Serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// I know this is useless but I am using it just to test 

public class SampleDatastreamConsumerKstream {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String APPLICATION_ID = "user-message-count";
    private static final String TOPIC_IN = "sample-datastream-raw";
    private static final String TOPIC_OUT = "sample-count-es";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static Properties createKStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/kafka-streams-tmp/kafka-streams");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    public static void main(String[] args) {
        // Create properties for Kafka stream
        Properties streamProperties = createKStreamProperties();
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream(TOPIC_IN,
                Consumed.with(Serdes.String(), Serdes.String()));

        inputStream
                .mapValues(value -> {

                    // System.out.println(value);
                    try {
                        // Parse JSON and extract the "username"
                        JsonNode recordContent = objectMapper.readTree(value);
                        return recordContent.has("username") ? recordContent.get("username").asText() : "";
                    } catch (Exception e) {
                        e.printStackTrace();
                        return ""; // Return empty if error occurs
                    }
                })
                .filter((key, value) -> !value.isEmpty())
                .selectKey((key, value) -> value)
                .groupByKey()
                .count()
                .toStream()
                .map((key, value) -> {
                    ObjectNode outputNode = objectMapper.createObjectNode();
                    outputNode.put("username", key);
                    outputNode.put("messageCount", value);
                    System.out.println(outputNode.toString());
                    return new KeyValue<>("{\"test\": 1}", outputNode.toString());
                })
                .to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.String()));
                
        // Build Topology
        Topology topology = streamsBuilder.build();

        CountDownLatch latch = new CountDownLatch(1);

        // Start stream
        try (KafkaStreams streams = new KafkaStreams(topology, streamProperties)) {
            Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });

            streams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
