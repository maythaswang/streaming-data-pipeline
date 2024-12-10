package com.datastreaming.utils;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * A utility class to delete all data from an Elasticsearch index.
 * This class sends a DELETE request to Elasticsearch to delete all data from the specified index.
 */

public class DeleteIndexData {

    // Set the index name
    private static final String INDEX = "sample-datastream-es";
    private static final String ELASTICSEARCH_HOST = "http://localhost:9200";

    public static void main(String[] args) {
        try {
            // Check if the index exists
            URL checkUrl = new URL(ELASTICSEARCH_HOST + "/" + INDEX);
            HttpURLConnection checkConnection = (HttpURLConnection) checkUrl.openConnection();
            checkConnection.setRequestMethod("HEAD");
            int checkResponseCode = checkConnection.getResponseCode();
            checkConnection.disconnect();

            if (checkResponseCode == 200) {
                // Create the URL for the DELETE request
                URL url = new URL(ELASTICSEARCH_HOST + "/" + INDEX + "/_delete_by_query");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);

                String jsonInputString = "{ \"query\": { \"match_all\": {} } }";
                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }

                int responseCode = connection.getResponseCode();
                System.out.println("Elasticsearch response code: " + responseCode);

                connection.disconnect();
            } else {
                System.out.println("Index " + INDEX + " does not exist.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}