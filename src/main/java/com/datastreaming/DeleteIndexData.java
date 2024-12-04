package com.datastreaming;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class DeleteIndexData {

    private static final String ELASTICSEARCH_HOST = "http://localhost:9200";
    private static final String INDEX = "to-elastic-search";

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

                // Set the request body
                String jsonInputString = "{ \"query\": { \"match_all\": {} } }";
                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = jsonInputString.getBytes("utf-8");
                    os.write(input, 0, input.length);
                }

                // Get the response
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