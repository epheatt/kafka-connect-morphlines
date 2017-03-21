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
  --data '{"name":"morphlines-solr-sink","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","topics":"twitter","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf"}}' \
  http://0.0.0.0:8082/connectors

curl -X POST -H "Content-Type: application/json" \
  --data '{"name": "quickstart-text-file-source", "config": {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector", "tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topic":"twitter", "file": "/etc/kafka-connect/jars/twitter.txt"}}' \
  http://0.0.0.0:8082/connectors
  
cat config/twitter.txt >> target/twitter.txt  
  
```

 

