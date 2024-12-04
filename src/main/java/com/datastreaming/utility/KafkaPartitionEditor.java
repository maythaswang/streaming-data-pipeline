package com.datastreaming.utility;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * A utility class to edit the number of partitions for a Kafka topic.
 */

public class KafkaPartitionEditor {

    // Set the partition and the topic you want to edit
    private static final int newPartitionCount = 2;
    private static final String TOPIC_IN = "anime_details_topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:29092");

        try (AdminClient adminClient = AdminClient.create(properties)) {


            Map<String, NewPartitions> newPartitions = Collections.singletonMap(TOPIC_IN, NewPartitions.increaseTo(newPartitionCount));

            adminClient.createPartitions(newPartitions).all().get();

            System.out.println("Successfully increased the number of partitions for topic: " + TOPIC_IN);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}