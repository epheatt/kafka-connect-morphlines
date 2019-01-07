/**
 * Copyright © 2017 Eric Pheatt (epheatt@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.epheatt.kafka.connect.morphlines;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Lists;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.eclipse.core.runtime.URIUtil;

import com.google.common.io.Resources;

import java.net.URI;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public final class KafkaDataProducerCommandBuilder implements CommandBuilder {
    private static final Logger log = LoggerFactory.getLogger(KafkaDataProducerCommandBuilder.class);
    
    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("kafkaProducer");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new KafkaDataProducer(this, config, parent, child, context);
    }
    
    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    /** Implementation that does logging and metrics */
    private static final class KafkaDataProducer extends AbstractCommand {
        private static final Logger log = LoggerFactory.getLogger(KafkaDataProducer.class);
        private static final ObjectMapper mapper = new ObjectMapper();
        
        private final Map<String, String> mappings = new HashMap<String, String>();
        private final String topic;
        private final String topicField;
        private final String partitionField;
        private final String timestampField;
        private final String keyField;
        private final String valueField;
        private final String keySchemaField;
        private final String valueSchemaField;
        private final Schema keyFixedSchema;
        private final Schema valueFixedSchema;
        private final Charset characterSet;
        private final String kafkaMethod;
        private final String kafkaRestUrl;
        //private final MorphlineProducer producer;
        private static final AvroData AVRO_CONVERTER;
        static {
            AVRO_CONVERTER = new AvroData(new AvroDataConfig.Builder()
                                            //.with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize)
                                            .build());
        }
        
        public KafkaDataProducer(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            this.topic = getConfigs().getString(config, "topic", null);
            this.topicField = getConfigs().getString(config, "topicField", null);
            if (topic == null && topicField == null) {
                throw new MorphlineCompilationException(
                  "Either topic or topicField must be defined", config);
              }
            
            this.partitionField = getConfigs().getString(config, "partitionField", null);
            this.timestampField = getConfigs().getString(config, "timestampField", null);
            this.keyField = getConfigs().getString(config, "keyField", null);
            this.keySchemaField = getConfigs().getString(config, "keySchemaField", null);
            String keySchemaString = getConfigs().getString(config, "keySchemaString", null);
            String keySchemaFile = getConfigs().getString(config, "keySchemaFile", null);
            
            int keyDefinitions = 0;
            if (keySchemaField != null) {
                keyDefinitions++;
            }
            if (keySchemaString != null) {
                keyDefinitions++;
            }
            if (keySchemaFile != null) {
                keyDefinitions++;
            }
            if (keyDefinitions > 1) {
              throw new MorphlineCompilationException(
                "Must define only one of keySchemaFile or keySchemaString or keySchemaField at the same time", config);
            }
            
            if (keySchemaString != null) {
                this.keyFixedSchema = getSchemaFor(keySchemaString);
            } else if (keySchemaFile != null) {
                try {
                    URI keySchemaUri;
                    if (keySchemaFile.startsWith("resource:")) {
                        keySchemaUri = Resources.getResource(keySchemaFile.substring(keySchemaFile.indexOf(":") + 1)).toURI();
                    } else {
                        keySchemaUri = URIUtil.fromString(keySchemaFile);                    
                    }
                    this.keyFixedSchema = getSchemaFor(URIUtil.toURL(keySchemaUri));
                } catch (IOException e) {
                    throw new MorphlineCompilationException("Cannot parse external Avro schema file: " + keySchemaFile, config, e);
                } catch (java.net.URISyntaxException use) {
                    throw new MorphlineCompilationException("Cannot parse external Avro schema file: " + keySchemaFile, config, use);                    
                }
            } else {
                this.keyFixedSchema = null;
            }
            
            this.valueSchemaField = getConfigs().getString(config, "valueSchemaField", null);
            String valueSchemaString = getConfigs().getString(config, "valueSchemaString", null);
            String valueSchemaFile = getConfigs().getString(config, "valueSchemaFile", null);
            
            int valueDefinitions = 0;
            if (valueSchemaField != null) {
                valueDefinitions++;
            }
            if (valueSchemaString != null) {
                valueDefinitions++;
            }
            if (valueSchemaFile != null) {
                valueDefinitions++;
            }
            if (valueDefinitions == 0) {
              throw new MorphlineCompilationException(
                "Either valueSchemaFile or valueSchemaString or valueSchemaField must be defined", config);
            }
            if (valueDefinitions > 1) {
              throw new MorphlineCompilationException(
                "Must define only one of valueSchemaFile or valueSchemaString or valueSchemaField at the same time", config);
            }
            
            if (valueSchemaString != null) {
                this.valueFixedSchema = getSchemaFor(valueSchemaString);
            } else if (valueSchemaFile != null) {
                try {
                    URI valueSchemaUri;
                    if (valueSchemaFile.startsWith("resource:")) {
                        valueSchemaUri = Resources.getResource(valueSchemaFile.substring(valueSchemaFile.indexOf(":") + 1)).toURI();
                    } else {
                        valueSchemaUri = URIUtil.fromString(valueSchemaFile);                    
                    }
                    this.valueFixedSchema = getSchemaFor(URIUtil.toURL(valueSchemaUri));
                } catch (IOException e) {
                    throw new MorphlineCompilationException("Cannot parse external Avro schema file: " + valueSchemaFile, config, e);
                } catch (java.net.URISyntaxException use) {
                    throw new MorphlineCompilationException("Cannot parse external Avro schema file: " + valueSchemaFile, config, use);                    
                }
            } else {
                this.valueFixedSchema = null;
            }
            
            this.valueField = getConfigs().getString(config, "valueField", null);
            this.characterSet = Charset.forName(getConfigs().getString(config, "characterSet", StandardCharsets.UTF_8.name()));
            
            Config mappingsConfig = getConfigs().getConfig(config, "mappings", ConfigFactory.empty());
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(mappingsConfig)) {
                mappings.put(entry.getKey(), entry.getValue().toString());
            }
            
            Config propsConfig = getConfigs().getConfig(config, "properties", ConfigFactory.empty());
            Properties props = new Properties();
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(propsConfig)) {
                props.put(entry.getKey().replaceAll("-", "."), entry.getValue());
            }
            
            this.kafkaRestUrl = getConfigs().getString(propsConfig, "kafka-rest-url", null);
            String kafkaMethod = getConfigs().getString(propsConfig, "method", null);
            if (kafkaRestUrl != null) { 
                this.kafkaMethod = "REST";
            } else {
                this.kafkaMethod = kafkaMethod;
            }
            String bootstrapServers = getConfigs().getString(propsConfig, "bootstrap-servers", null);
            String schemaRegistryUrl = getConfigs().getString(propsConfig, "schema-registry-url", null);
            if (kafkaRestUrl == null && (bootstrapServers == null || schemaRegistryUrl == null)) {
                throw new MorphlineCompilationException(
                        "producer bootstrap-servers and schema-registry-url must be defined", config);
            }
            
            validateArguments();
            
            if (!props.containsKey("acks")) props.put("acks", "all");
            if (!props.containsKey("retries")) props.put("retries", 0);
            if (!props.containsKey("key.serializer")) props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            if (!props.containsKey("value.serializer")) props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            if (kafkaMethod == null && bootstrapServers != null && schemaRegistryUrl != null)
                context.getSettings().put("kafkaProducerProps", props);
            //this.producer = new MorphlineProducer(props);
        }
        
        @Override
        protected boolean doProcess(Record inputRecord) {
            String topic = null;
            if (this.topic == null && topicField != null) {
                topic = (String) inputRecord.getFirstValue(topicField);
            } else {
                topic = this.topic;
            }
            log.debug("producer topic: " + topic);
            Integer partition = null;
            if (partitionField != null) {
                partition = Integer.valueOf((String) inputRecord.getFirstValue(partitionField));
            }
            log.debug("producer partition: " + partition);
            Long timestamp = null;
            if (timestampField != null) {
                timestamp = Long.valueOf((String) inputRecord.getFirstValue(timestampField));
            }
            log.debug("producer timestamp: " + timestamp);
            Object key = null;
            if (keyField != null) {
                key = inputRecord.getFirstValue(keyField);
            }
            log.debug("producer key: " + key);
            Preconditions.checkNotNull(topic);
            Schema keySchema;
            if (keySchemaField != null) {
            	Object schema = inputRecord.getFirstValue(keySchemaField);
            	if(schema instanceof org.apache.kafka.connect.data.Schema)
            		keySchema = AVRO_CONVERTER.fromConnectSchema((org.apache.kafka.connect.data.Schema) schema);
            	else
            		keySchema = (Schema) schema;
                Preconditions.checkNotNull(keySchema);
            } else {
                keySchema = keyFixedSchema;
            }
            log.debug("producer keySchema: " + keySchema);
            Schema valueSchema;
            if (valueSchemaField != null) {
            	Object schema = inputRecord.getFirstValue(valueSchemaField);
            	if(schema instanceof org.apache.kafka.connect.data.Schema)
            		valueSchema = AVRO_CONVERTER.fromConnectSchema((org.apache.kafka.connect.data.Schema) schema);
            	else
            		valueSchema = (Schema) schema;
                Preconditions.checkNotNull(valueSchema);
            } else {
                valueSchema = valueFixedSchema;
            }
            log.debug("producer valueSchema: " + valueSchema);
            Object value = inputRecord.getFirstValue(valueField != null ? valueField : Fields.ATTACHMENT_BODY);
            log.debug("producer value: " + value);
            ProducerData data = new ProducerData(new KafkaRecord(value, key), (keySchema != null ? keySchema.toString() : null), null, valueSchema.toString(), null);
            if (kafkaMethod == null && getContext().getSettings().get("kafkaProducerProps") != null && getContext().getSettings().get("kafkaProducer") != null) {
                MorphlineProducer producer = (MorphlineProducer) getContext().getSettings().get("kafkaProducer");
                producer.publish(topic, partition, timestamp, keySchema, key, valueSchema, value);
            } else {
                Object response = publish(mapper, kafkaRestUrl, topic, partition, timestamp, data);
                log.debug("producer response: " + response);
                if (response != null && response instanceof ProducerException) {
                    if (((ProducerException) response).errorCode == 50003) {
                        //retry once
                        response = publish(mapper, kafkaRestUrl, topic, partition, timestamp, data);
                        if (response != null && response instanceof ProducerException) {
                            throw (ProducerException) response;
                        }
                    } else {
                        throw (ProducerException) response;
                    }
                }
            }
            return super.doProcess(inputRecord);
        }
        
        private static Object publish(ObjectMapper mapper, String baseURI, String topic, Integer partition, Long timestamp, ProducerData data) {
            HttpClient httpClient = HttpClientBuilder.create().build();
            Object result = new Object();
            //http://localhost:8083/topics/{topic}/partition/{partition}
            StringBuilder requestUri = new StringBuilder(baseURI);
            requestUri.append("/topics/").append(topic);
            if (partition != null) {
                requestUri.append("/partition/").append(partition);
            }
            try {
                HttpPost post = new HttpPost(new URI(requestUri.toString()));
                post.addHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/vnd.kafka.avro.v2+json"));
                post.addHeader(new BasicHeader(HttpHeaders.ACCEPT, "application/vnd.kafka.v2+json"));
                post.setEntity(new StringEntity(mapper.writeValueAsString(data), ContentType.APPLICATION_JSON));
                HttpResponse response = httpClient.execute(post);
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    try {
                        InputStream okStream = response.getEntity().getContent();
                        result = mapper.readValue(okStream, ProducerResponse.class);
                        okStream.close();
                    } catch (IOException exception) {
                        
                    }
                } else {
                    try {
                        InputStream errStream = response.getEntity().getContent();
                        result = mapper.readValue(errStream, ProducerException.class);
                        errStream.close();
                    } catch (IOException exception) {
                        
                    }
                }
            } catch (Exception e) {
                
            } finally {
                //httpClient.getConnectionManager().shutdown();
            }
            return result;
        }
        
        private static Schema getSchemaFor(String str) {
            Parser parser = new Parser();
            Schema schema = parser.parse(str);
            return schema;
        }

        private static Schema getSchemaFor(File file) {
            Parser parser = new Parser();
            Schema schema;
            try {
              schema = parser.parse(file);
            } catch (IOException e) {
              throw new RuntimeException("Failed to parse Avro schema from " + file.getName(), e);
            }
            return schema;
          }

        private static Schema getSchemaFor(InputStream stream) {
            Parser parser = new Parser();
            Schema schema;
            try {
                schema = parser.parse(stream);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse Avro schema", e);
            }
            return schema;
        }

        private static Schema getSchemaFor(URL url) {
            InputStream in = null;
            try {
                in = url.openStream();
                return getSchemaFor(in);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse Avro schema", e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
        }
    }
    
    private static class KafkaOffset {
        public final int partition;
        public final long offset;
        public final Long errorCode;
        public final String error;

        @JsonCreator
        public KafkaOffset(@JsonProperty("partition") int partition,
                @JsonProperty("offset") long offset,
                @JsonProperty("error_code") long errorCode,
                @JsonProperty("error") String error) {
            this.partition = partition;
            this.offset = offset;
            this.errorCode = errorCode;
            this.error = error;
        }
    }
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class ProducerData {
        @JsonProperty("records")
        public final List<KafkaRecord> records;
        @JsonProperty("key_schema")
        public final String keySchema;
        @JsonProperty("key_schema_id")
        public final Integer keySchemaId;
        @JsonProperty("value_schema")
        public final String valueSchema;
        @JsonProperty("value_schema_id")
        public final Integer valueSchemaId;

        public ProducerData(KafkaRecord record, String keySchema, Integer keySchemaId, String valueSchema,
                Integer valueSchemaId) {
            this(Lists.newArrayList(record), keySchema, keySchemaId,  valueSchema, valueSchemaId);
        }

        public ProducerData(List<KafkaRecord> records, String keySchema, Integer keySchemaId, String valueSchema,
                Integer valueSchemaId) {
            this.records = records;
            this.keySchema = keySchema;
            this.keySchemaId = keySchemaId;
            this.valueSchema = valueSchema;
            this.valueSchemaId = valueSchemaId;
        }
    }

    private static class ProducerResponse {
        public final List<KafkaOffset> offsets;
        public final Integer keySchemaId;
        public final Integer valueSchemaId;

        @JsonCreator
        public ProducerResponse(@JsonProperty("offsets") List<KafkaOffset> offsets,
                @JsonProperty("key_schema_id") Integer keySchemaId,
                @JsonProperty("value_schema_id") Integer valueSchemaId) {
            this.offsets = offsets;
            this.keySchemaId = keySchemaId;
            this.valueSchemaId = valueSchemaId;
        }
    }
    
    private static class ProducerException extends RuntimeException {
        private Long errorCode;

        @JsonCreator
        public ProducerException(@JsonProperty("message") String message, @JsonProperty("error_code") Long errorCode) {
            super(message);
            this.errorCode = errorCode;
        }

        public Long getErrorCode() {
            return errorCode;
        }
    }
    
    private static class KafkaTopic {
        public final String name;
        public final int partitions;

        @JsonCreator
        public KafkaTopic(@JsonProperty("name") String name, @JsonProperty("partitions") int partitions) {
            this.name = name;
            this.partitions = partitions;
        }
    }
    
    private static class KafkaRecord<K, V> {
        public final K key;
        public final V value;

        public KafkaRecord(V value) {
            this(value, null);
        }

        public KafkaRecord(V value, K key) {
            this.value = value;
            this.key = key;
        }
    }
    
}