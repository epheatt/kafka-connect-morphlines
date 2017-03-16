# Running in development

There is also a docker-compose script with configuration for zookeeper, kafka and the confluent schema registry. This can be used with `docker-compose up`

```
mvn clean package
export CLASSPATH="$(find `pwd`/target/kafka-connect-morphlines-1.0.0-SNAPSHOT-package/share/java/kafka-connect-morphlines -type f | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/morphlines.conf
```



 

