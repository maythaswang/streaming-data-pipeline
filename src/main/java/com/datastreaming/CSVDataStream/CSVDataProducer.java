package com.datastreaming.CSVDataStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class CSVDataProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC_OUT = "csv-raw";
    private static final String CSV_FILE_PATH = "src/main/resources/data/anime_filtered.csv";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = buildProducer();
        producer.initTransactions();

        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
            String line;
            br.readLine(); // Skip the header line
            producer.beginTransaction();
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                String key = fields[0]; // Use anime_id as the key
                ObjectNode jsonNode = objectMapper.createObjectNode();
                jsonNode.put("anime_id", Integer.parseInt(fields[0]));
                jsonNode.put("title", fields[1]);

                String jsonString = objectMapper.writeValueAsString(jsonNode);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_OUT, key, jsonString);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Sent record to topic " + metadata.topic() + " partition " + metadata.partition() + " with offset " + metadata.offset());
                    }
                });
            }
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }
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
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "csv-data-stream-processor");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 200);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }
}