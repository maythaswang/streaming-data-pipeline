package com.datastreaming.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.Set;

public class ListKafkaTopics {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            Set<String> topics = adminClient.listTopics().names().get();
            System.out.println("Kafka Topics:");
            for (String topic : topics) {
                System.out.println(topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}