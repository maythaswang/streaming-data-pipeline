package com.datastreaming.utils;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicManager {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final Properties adminClientProperties = createAdminClientProperties();

    private static Properties createAdminClientProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static int countTopicPartitions(String topicName) {
        int partitionsCount = -1;

        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            DescribeTopicsResult result = adminClient.describeTopics(java.util.Collections.singletonList(topicName));
            Map<String, TopicDescription> topicDescriptionMap = result.all().get();

            TopicDescription topicDescription = topicDescriptionMap.get(topicName);
            partitionsCount = topicDescription.partitions().size();
            System.out.println("Topic: " + topicDescription.name() + "\nNumber of Partitions: " + partitionsCount);

        } catch (Exception e) {
            e.printStackTrace();

        }

        return partitionsCount;
    }

    public static Set<String> getAllKafkaTopics() {
        Set<String> topics = null;

        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            topics = adminClient.listTopics().names().get();
            System.out.println("Kafka Topics:");
            for (String topic : topics) {
                System.out.println(topic);
            }

        } catch (Exception e) {
            e.printStackTrace();

        }

        return topics;
    }

    public static boolean increaseTopicPartitions(String topicName, int newPartitionCount, short replicationFactor) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            boolean topicExists;
            int currentPartitionCount = 0;
            try {
                Map<String, TopicDescription> topicDescriptionMap = adminClient
                        .describeTopics(Collections.singletonList(topicName)).all().get();
                topicExists = topicDescriptionMap.containsKey(topicName);
                if (topicExists) {
                    currentPartitionCount = topicDescriptionMap.get(topicName).partitions().size();
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                    topicExists = false;
                } else {
                    throw e;
                }
            }

            if (!topicExists) {
                NewTopic newTopic = new NewTopic(topicName, newPartitionCount, replicationFactor);
                try {
                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                    System.out.println("Successfully created the topic: " + topicName);
                    return true;

                } catch (TopicExistsException e) {
                    System.out.println("Topic already exists: " + topicName);

                }

            } else if (newPartitionCount > currentPartitionCount) {
                Map<String, NewPartitions> newPartitions = Collections.singletonMap(topicName,
                        NewPartitions.increaseTo(newPartitionCount));
                adminClient.createPartitions(newPartitions).all().get();
                System.out.println("Successfully increased the number of partitions for topic: " + topicName);

                return true;

            } else {
                System.out.println("The topic already has " + currentPartitionCount
                        + " partitions, which is equal to or greater than the requested " + newPartitionCount
                        + " partitions.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public static boolean deleteTopic(String topicName) {
        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            deleteTopicsResult.all().get();
            System.out.println("Successfully deleted the topic: " + topicName);
            return true;

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();

        }
        return false;

    }
}
