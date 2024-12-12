# Streaming-Data-Pipeline
----

Contributor: Achira Laovong, Maythas Wangcharoenwong


0. Snippet for testing throughput 
- The content are stored under `_SampleDatastreamTest` directory 
- `SampleDatastreamProducer.java` -> \[`SampleDatastreamConsumer.java`, `SampleDatastreamConsumerKstream.java`\] which will write to \[`sample-datastream-es`, `sample-count-es`\] accordingly. 

1. Version \[1\]
- Please use the specified application in MALTopRaw and AnimeDetails DIR
- `MALTopRawProducer.java` -> `MALTopRawConsumer.java` -> `AnimeDetailsConsumer.java` which will then write to elasticsearch topic `to-elastic-search`

2. Version \[2\]
- Please use the specified application in CSVDataStream
- `CSVDataProducer.java` -> `CSVDataConsumer.java` -> `CSVDataFamilyFriendlyFilter.java` which will then write to elasticsearch topic `es-anime-data`

3. Version \[3\]
- Please use the specified application in CSVDataStream
- `CSVDataProducer.java` -> `CSVKStreamProcessor.java` which will then write to elasticsearch topic `es-anime-data`