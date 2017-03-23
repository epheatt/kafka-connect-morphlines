# Running in development

```
mvn clean package
export CLASSPATH="$(find `pwd`/target/kafka-connect-morphlines-1.0.0-SNAPSHOT-package/share/java/kafka-connect-morphlines -type f | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/morphlines.conf
```


There is also a docker-compose script with configuration for zookeeper, kafka and the confluent schema registry. This can be used with `docker-compose up`

```
mvn clean package
rm target/original-kafka-connect-morphlines-0.1-SNAPSHOT.jar
cp config/cloudsolr.conf target/
docker-compose up kafka-connect

curl -X POST -H "Content-Type: application/json" \
  --data '{"name":"morphlines-solr-sink-string","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topics":"twitter-string","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf"}}' \
  http://0.0.0.0:8082/connectors

curl -X POST -H "Content-Type: application/json" \
  --data '{"name": "quickstart-text-file-source", "config": {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector", "tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topic":"twitter-string", "file": "/etc/kafka-connect/jars/twitter-string.txt"}}' \
  http://0.0.0.0:8082/connectors
  
cat config/twitter.txt >> target/twitter-string.txt  

curl -X POST -H "Content-Type: application/json" \
  --data '{"name":"morphlines-solr-sink-avro","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"topics":"twitter-avro","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf"}}' \
  http://0.0.0.0:8082/connectors

docker run -it \
  --link=kafkaconnectmorphlines_schema-registry_1:schema-registry \
  --link=kafkaconnectmorphlines_kafka_1:kafka \
  --rm \
  confluentinc/cp-schema-registry:3.2.0 bash

/usr/bin/kafka-avro-console-producer   --broker-list kafka:9092 --property schema.registry.url=http://schema-registry:8081 --topic twitter-string  --property value.schema='{"type":"string"}'
"{""name"":""quickstart-string-console-source""}"
"{\"name\":\"quickstart-text-console-source\"}"

/usr/bin/kafka-avro-console-producer   --broker-list kafka:9092 --property schema.registry.url=http://schema-registry:8081 --topic twitter-avro  --property value.schema='{"type":"record","name":"twitter","fields":[{"name":"name","type":"string"}]}'
{"name":"quickstart-avro-console-source"}

/usr/bin/kafka-avro-console-consumer    --bootstrap-server kafka:9092 --topic twitter-avro --property schema.registry.url=http://schema-registry:8081


curl -X POST -H "Content-Type: application/json" \
  --data '{"name":"morphlines-solr-sink-json","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topics":"twitter-json","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf"}}' \
  http://0.0.0.0:8082/connectors
  
docker run -it \
  --link=kafkaconnectmorphlines_kafka_1:kafka \
  --rm \
  confluentinc/cp-kafka:3.2.0 bash
  
/usr/bin/kafka-console-producer   --broker-list kafka:9092 --topic twitter-json
{"name":"quickstart-json-console-source"}


```

 

