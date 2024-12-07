package com.datastreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

/**
 * A Kafka producer that reads the top anime from the MyAnimeList API and sends it to a Kafka topic.
 * Example using curl: curl -X GET "https://api.myanimelist.net/v2/anime/ranking?ranking_type=all&limit=2" -H "X-MAL-CLIENT-ID: {client_id}"
 * More info: https://myanimelist.net/apiconfig/references/api/v2#operation/anime_ranking_get
 */

public class MALTopRawProducer {

    private static final int LIMIT = 500;
    private static final int TOTAL_ANIME = 5000;
    private static final String API_URL_TEMPLATE = "https://api.myanimelist.net/v2/anime/ranking?ranking_type=all&limit=" + LIMIT + "&offset=%d";
    private static final String TOPIC = "top_anime_topic";

    public static void main(String[] args) {
        try {
            // Load properties from config file
            Properties configProps = new Properties();
            try (InputStream input = MALTopRawProducer.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (input == null) {
                    System.out.println("Failed to load config.properties");
                    return;
                }
                configProps.load(input);
            }

            String clientId = configProps.getProperty("client_id");

            // Kafka Producer configuration
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            for (int offset = 0; offset < TOTAL_ANIME; offset += LIMIT) {
                String apiUrl = String.format(API_URL_TEMPLATE, offset);

                // Query the MyAnimeList API
                URL url = new URL(apiUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("X-MAL-CLIENT-ID", clientId);

                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                connection.disconnect();

                // Print the API response
                System.out.println("API Response: " + content);

                // Send the API response to the Kafka topic
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, content.toString());
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("Error sending message: " + exception.getMessage());
                    } else {
                        System.out.println("Message sent to topic " + metadata.topic() +
                                " partition " + metadata.partition() +
                                " with offset " + metadata.offset());
                    }
                });
            }

            // Ensure all messages are sent before closing
            producer.flush();
            producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}