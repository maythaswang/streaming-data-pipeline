curl -X POST "http://localhost:28083/connectors" \
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

curl -X POST "http://localhost:28083/connectors" \
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

curl -X POST "http://localhost:28083/connectors" \
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