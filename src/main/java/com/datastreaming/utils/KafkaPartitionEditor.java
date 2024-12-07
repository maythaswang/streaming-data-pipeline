package com.datastreaming.utils;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * A utility class to edit the number of partitions for a Kafka topic.
 * The edited topic should have higher partition count than the current topic.
 * If the topic does not exist, it will be created with the specified partition count.
 * If the topic already exists, the partition count will be increased to the specified partition count.
 * If you need to decrease the partition count, you will need to delete the topic and create a new one.
 */

public class KafkaPartitionEditor {

    // Set the partition and the topic you want to edit
    private static final int newPartitionCount = 1;
    private static final String TOPIC_IN = "anime_details_topic";
    private static final short REPLICATION_FACTOR = 1;
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            boolean topicExists;
            int currentPartitionCount = 0;
            try {
                Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(Collections.singletonList(TOPIC_IN)).all().get();
                topicExists = topicDescriptionMap.containsKey(TOPIC_IN);
                if (topicExists) {
                    currentPartitionCount = topicDescriptionMap.get(TOPIC_IN).partitions().size();
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                    topicExists = false;
                } else {
                    throw e;
                }
            }

            if (!topicExists) {
                NewTopic newTopic = new NewTopic(TOPIC_IN, newPartitionCount, REPLICATION_FACTOR);
                try {
                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                    System.out.println("Successfully created the topic: " + TOPIC_IN);
                } catch (TopicExistsException e) {
                    System.out.println("Topic already exists: " + TOPIC_IN);
                }
            } else if (newPartitionCount > currentPartitionCount) {
                Map<String, NewPartitions> newPartitions = Collections.singletonMap(TOPIC_IN, NewPartitions.increaseTo(newPartitionCount));
                adminClient.createPartitions(newPartitions).all().get();
                System.out.println("Successfully increased the number of partitions for topic: " + TOPIC_IN);
            } else {
                System.out.println("The topic already has " + currentPartitionCount + " partitions, which is equal to or greater than the requested " + newPartitionCount + " partitions.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}