package com.datastreaming;

import com.datastreaming.utils.KafkaTopicManager;

public class KafkaConfig {
    private static short REPLICATION_FACTOR = 1;
    public static void main(String args[]) {

        // KafkaTopicManager.deleteTopic("sample-datastream-raw");
        // KafkaTopicManager.deleteTopic("sample-count-es");
        KafkaTopicManager.increaseTopicPartitions("sample-datastream-raw", 3, REPLICATION_FACTOR);
        // KafkaTopicManager.increaseTopicPartitions("sample-count-es", 3, REPLICATION_FACTOR);
        KafkaTopicManager.increaseTopicPartitions("sample-datastream-es", 3, REPLICATION_FACTOR);
        KafkaTopicManager.countTopicPartitions("sample-datastream-raw");
        KafkaTopicManager.countTopicPartitions("sample-datastream-es");
        // KafkaTopicManager.countTopicPartitions("sample-count-es");
        // KafkaTopicManager.deleteTopic("user-message-count-KSTREAM-AGGREGATE-STATE-STORE-0000000004-changelog");
        // KafkaTopicManager.deleteTopic("user-message-count-KSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition");
        KafkaTopicManager.getAllKafkaTopics();
        // KafkaTopicManager.deleteTopic("sample-datastream-raw");
        // KafkaTopicManager.deleteTopic("sample-datastream-es");
    }
}
