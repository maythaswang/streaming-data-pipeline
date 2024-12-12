### Basic Instructions on how to setup connector for elasticsearch-sink

---

※ You only need to do this one time 

[1] Right now, the topic name for sending from kafka to the sink is `to-elastic-search`, you can simply use them from normal producer node without any extra configurations.

[2] To access the content on elastic search use whatever is written in the `topics:` field which right now is `to-elastic-search` (for some reason index doesn't work)

1.  Access the container
    `winpty docker exec -it <kafka-connect-container> bash`

2.  Run the following snippet (use option2 and yes to all)

```bash
confluent-hub install confluentinc/kafka-connect-elasticsearch:latest
```

3. Restart the container

4. Run the following snippet

Simply run `es_sink_setup.sh`

You can ignore the rest.

------
__ old version

4.1 Sink for "to-elastic-searech"

```bash
curl -X POST "http://172.20.0.14:28083/connectors" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "elasticsearch-sink",
        "config": {
          "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
          "tasks.max": "1",
          "topics": "to-elastic-search",
          "key.ignore": "true",
          "schema.ignore": "true",
          "connection.url": "http://172.20.0.12:9200",
          "type.name": "_doc",
          "key.converter": "org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable": "false",
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter.schemas.enable": "false",
          "index": "sample_index"
        }
      }'
```

4.2 Sink for `sample-datastream-es`

```bash
curl -X POST "http://172.20.0.14:28083/connectors" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "elasticsearch-sink-sample-datastream",
        "config": {
          "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
          "tasks.max": "2",
          "topics": "sample-datastream-es",
          "key.ignore": "true",
          "schema.ignore": "true",
          "connection.url": "http://172.20.0.12:9200",
          "type.name": "_doc",
          "key.converter": "org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable": "false",
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter.schemas.enable": "false",
          "index": "sample_index"
        }
      }'
```
4.3 Sink for `sample-count-es`
```bash
curl -X POST "http://172.20.0.14:28083/connectors" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "elasticsearch-sink-sample-count",
        "config": {
          "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
          "tasks.max": "2",
          "topics": "sample-count-es",
          "key.ignore": "true",
          "schema.ignore": "true",
          "connection.url": "http://172.20.0.12:9200",
          "type.name": "_doc",
          "key.converter": "org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable": "false",
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter.schemas.enable": "false",
          "index": "sample_index"
        }
      }'
  ```