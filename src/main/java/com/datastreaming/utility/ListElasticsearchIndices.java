package com.datastreaming.utility;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

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
            System.out.println(content);

            // Retrieve 5 example documents from the sample_index
            url = new URL(ELASTICSEARCH_HOST + "/sample_index/_search?size=5");
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            connection.disconnect();

            System.out.println("Example documents from index sample_index:");
            System.out.println(content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}