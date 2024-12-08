package com.datastreaming.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import java.util.List;
import java.util.Map;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class ListElasticsearchIndices {

    private static final String ELASTICSEARCH_HOST = "http://localhost:9200";

    public static void main(String[] args) {
        try {
            // List all indices
            URL url = new URL(ELASTICSEARCH_HOST + "/_cat/indices?v&format=json");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            connection.disconnect();

            System.out.println("Elasticsearch Indices:");
            // Create an ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            try {
                // Parse the JSON string into a List of Map
                List<Map<String, Object>> jsonList = objectMapper.readValue(content.toString(),
                        new TypeReference<List<Map<String, Object>>>() {
                        });

                // Iterate and print each element in the list
                for (int i = 0; i < jsonList.size(); i++) {
                    Map<String, Object> item = jsonList.get(i);
                    System.out.println("Index " + i + "(" + item.get("index") + "): " + item);
                }
            } catch (IOException e) {
                // Handle any errors during parsing
                System.out.println("Error parsing JSON: " + e.getMessage());
            }

            // Retrieve 5 example documents from the sample_index
            // url = new URL(ELASTICSEARCH_HOST + "/sample_index/_search?size=5");
            // connection = (HttpURLConnection) url.openConnection();
            // connection.setRequestMethod("GET");

            // in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            // content = new StringBuilder();
            // while ((inputLine = in.readLine()) != null) {
            //     content.append(inputLine);
            // }
            // in.close();
            // connection.disconnect();

            // System.out.println("Example documents from index sample_index:");
            // System.out.println(content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}