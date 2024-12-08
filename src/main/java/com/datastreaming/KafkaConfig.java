package com.datastreaming;

import com.datastreaming.utils.KafkaTopicManager;

public class KafkaConfig {
    private static short REPLICATION_FACTOR = 1;
    public static void main(String args[]) {
        KafkaTopicManager.increaseTopicPartitions("sample-datastream-raw", 3, REPLICATION_FACTOR);
        KafkaTopicManager.countTopicPartitions("sample-datastream-raw");
    }
}
