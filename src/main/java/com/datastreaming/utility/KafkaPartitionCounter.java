package com.datastreaming.utility;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Properties;

/**
 * A utility class to count the number of partitions for a Kafka topic.
 */

public class KafkaPartitionCounter {

    // Set the topic you want to count partitions for
    private static final String TOPIC_IN = "anime_details_topic";

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:29092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        AdminClient adminClient = AdminClient.create(properties);

        DescribeTopicsResult result = adminClient.describeTopics(java.util.Collections.singletonList(TOPIC_IN));
        Map<String, TopicDescription> topicDescriptionMap = result.all().get();

        TopicDescription topicDescription = topicDescriptionMap.get(TOPIC_IN);
        System.out.println("Topic: " + topicDescription.name());
        System.out.println("Number of Partitions: " + topicDescription.partitions().size());

        adminClient.close();
    }
}
