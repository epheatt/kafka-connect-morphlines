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

docker exec -it kafkaconnectmorphlines_solr_1 bin/solr create -c twitter
curl -X POST -H 'Content-type:application/json' --data-binary '{
  "delete-dynamic-field":{ "name":"*_phon_en" },
  "delete-field-type":{ "name":"phonetic_en" },
  "delete-dynamic-field":{ "name":"*_txt_ar" },
  "delete-field-type":{ "name":"text_ar" },
  "delete-dynamic-field":{ "name":"*_txt_bg" },
  "delete-field-type":{ "name":"text_bg" },
  "delete-dynamic-field":{ "name":"*_txt_ca" },
  "delete-field-type":{ "name":"text_ca" },
  "delete-dynamic-field":{ "name":"*_txt_cz" },
  "delete-field-type":{ "name":"text_cz" },
  "delete-dynamic-field":{ "name":"*_txt_da" },
  "delete-field-type":{ "name":"text_da" },
  "delete-dynamic-field":{ "name":"*_txt_de" },
  "delete-field-type":{ "name":"text_de" },
  "delete-dynamic-field":{ "name":"*_txt_el" },
  "delete-field-type":{ "name":"text_el" },
  "delete-dynamic-field":{ "name":"*_txt_es" },
  "delete-field-type":{ "name":"text_es" },
  "delete-dynamic-field":{ "name":"*_txt_eu" },
  "delete-field-type":{ "name":"text_eu" },
  "delete-dynamic-field":{ "name":"*_txt_fa" },
  "delete-field-type":{ "name":"text_fa" },
  "delete-dynamic-field":{ "name":"*_txt_fi" },
  "delete-field-type":{ "name":"text_fi" },
  "delete-dynamic-field":{ "name":"*_txt_fr" },
  "delete-field-type":{ "name":"text_fr" },
  "delete-dynamic-field":{ "name":"*_txt_ga" },
  "delete-field-type":{ "name":"text_ga" },
  "delete-dynamic-field":{ "name":"*_txt_gl" },
  "delete-field-type":{ "name":"text_gl" },
  "delete-dynamic-field":{ "name":"*_txt_hi" },
  "delete-field-type":{ "name":"text_hi" },
  "delete-dynamic-field":{ "name":"*_txt_hu" },
  "delete-field-type":{ "name":"text_hu" },
  "delete-dynamic-field":{ "name":"*_txt_hy" },
  "delete-field-type":{ "name":"text_hy" },
  "delete-dynamic-field":{ "name":"*_txt_id" },
  "delete-field-type":{ "name":"text_id" },
  "delete-dynamic-field":{ "name":"*_txt_it" },
  "delete-field-type":{ "name":"text_it" },
  "delete-dynamic-field":{ "name":"*_txt_ja" },
  "delete-field-type":{ "name":"text_ja" },
  "delete-dynamic-field":{ "name":"*_txt_lv" },
  "delete-field-type":{ "name":"text_lv" },
  "delete-dynamic-field":{ "name":"*_txt_nl" },
  "delete-field-type":{ "name":"text_nl" },
  "delete-dynamic-field":{ "name":"*_txt_no" },
  "delete-field-type":{ "name":"text_no" },
  "delete-dynamic-field":{ "name":"*_txt_pt" },
  "delete-field-type":{ "name":"text_pt" },
  "delete-dynamic-field":{ "name":"*_txt_ro" },
  "delete-field-type":{ "name":"text_ro" },
  "delete-dynamic-field":{ "name":"*_txt_ru" },
  "delete-field-type":{ "name":"text_ru" },
  "delete-dynamic-field":{ "name":"*_txt_sv" },
  "delete-field-type":{ "name":"text_sv" },
  "delete-dynamic-field":{ "name":"*_txt_th" },
  "delete-field-type":{ "name":"text_th" },
  "delete-dynamic-field":{ "name":"*_txt_tr" },
  "delete-field-type":{ "name":"text_tr" }
}' http://localhost:8983/solr/twitter/schema
curl http://localhost:8983/solr/twitter/config -H 'Content-type:application/json' -d'{
    "set-property" : {
    	"updateHandler.autoSoftCommit.maxTime":1000
	}
}'

docker exec -it kafkaconnectmorphlines_solr_1 bin/solr zk cp zk:/configs/twitter/managed-schema zk:/configs/twitter/schema.xml -z zookeeper:2181

curl -X POST -H "Content-Type: application/json" \
  --data '{"name":"morphlines-solr-sink","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topics":"twitter-string","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf","morphlines.zkHost":"zookeeper:2181","morphlines.collection":"twitter"}}' \
  http://0.0.0.0:8082/connectors

curl -X POST -H "Content-Type: application/json" \
  --data '{"name": "quickstart-text-file-source", "config": {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector", "tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topic":"twitter-string", "file": "/etc/kafka-connect/jars/twitter-string.txt"}}' \
  http://0.0.0.0:8082/connectors
  
cat config/twitter.txt >> target/twitter-string.txt  

curl -X POST -H "Content-Type: application/json" \
  --data '{"name": "quickstart-json-file-source", "config": {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector", "tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topic":"twitter-json", "file": "/etc/kafka-connect/jars/twitter-json.txt"}}' \
  http://0.0.0.0:8082/connectors

curl -X POST -H "Content-Type: application/json" \
  --data '{"name":"morphlines-solr-sink-avro","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"topics":"twitter-avro","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf","morphlines.zkHost":"zookeeper:2181","morphlines.collection":"twitter"}}' \
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
  --data '{"name":"morphlines-solr-sink-json","config":{"connector.class":"com.github.epheatt.kafka.connect.morphlines.MorphlineSinkConnector", "tasks.max":1,"key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":false,"value.converter.schemas.enable":false,"topics":"twitter-json","morphlines.morphlineId":"cloudsolr","morphlines.morphlineFile":"file:/etc/kafka-connect/jars/cloudsolr.conf","morphlines.zkHost":"zookeeper:2181","morphlines.collection":"twitter"}}' \
  http://0.0.0.0:8082/connectors
  
docker run -it \
  --link=kafkaconnectmorphlines_kafka_1:kafka \
  --rm \
  confluentinc/cp-kafka:3.2.0 bash
  
/usr/bin/kafka-console-producer   --broker-list kafka:9092 --topic twitter-json
{"name":"quickstart-json-console-source"}


```

 

