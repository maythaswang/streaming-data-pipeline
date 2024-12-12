# Basic Instructions on how to setup connector for elasticsearch-sink

※ You only need to do this when setting up first time (given that you did not delete your volumes subsequently.)

-----
### To setup elasticsearch sink, please do the following

1.  Access the container
    `winpty docker exec -it <kafka-connect-container> bash`

2.  Run the following snippet (use option2 and yes to all)

```bash
confluent-hub install confluentinc/kafka-connect-elasticsearch:latest
```

3. Restart the container

4. Run the following snippet

Simply run `es_sink_setup.sh`

-----

### Sink information

\[Sink 1\]
- Topic and index set to `to-elastic-search` 
- Maximum of 1 threads

\[Sink 2\]
- Topic and index set to `sample-datastream-es` 
- Maximum of 3 threads

\[Sink 3\]
- Topic and index set to `sample-count-es` 
- Maximum of 3 threads

\[Sink 4\]
- Topic and index set to `es-anime-data` 
- Maximum of 1 threads

※ This sink is the one currently used in the data pipelines, the rest are simply residuals incase one wishes to run older versions

------
