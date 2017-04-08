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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

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

        private final Map<String, String> mappings = new HashMap<String, String>();
        private final String topic;
        private final String topicField;
        private final String partitionField;
        private final String timestampField;
        private final String keyField;
        private final String schemaField;
        private final org.apache.avro.Schema fixedSchema;
        private final String valueField;
        private final Charset characterSet;
        private final MorphlineProducer producer;
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
            this.partitionField = getConfigs().getString(config, "partitionField", null);
            this.timestampField = getConfigs().getString(config, "timestampField", null);
            this.keyField = getConfigs().getString(config, "keyField", null);
            this.schemaField = getConfigs().getString(config, "schemaField", null);
            String schemaString = getConfigs().getString(config, "schemaString", null);
            String schemaFile = getConfigs().getString(config, "schemaFile", null);
            
            int numDefinitions = 0;
            if (schemaField != null) {
                numDefinitions++;
            }
            if (schemaString != null) {
                numDefinitions++;
            }
            if (schemaFile != null) {
                numDefinitions++;
            }
            if (numDefinitions == 0) {
              throw new MorphlineCompilationException(
                "Either schemaFile or schemaString or schemaField must be defined", config);
            }
            if (numDefinitions > 1) {
              throw new MorphlineCompilationException(
                "Must define only one of schemaFile or schemaString or schemaField at the same time", config);
            }
            
            if (schemaString != null) {
                this.fixedSchema = getSchemaFor(schemaString);
            } else if (schemaFile != null) {
                try {
                    URI schemaUri;
                    if (schemaFile.startsWith("resource:")) {
                        schemaUri = Resources.getResource(schemaFile.substring(schemaFile.indexOf(":") + 1)).toURI();
                    } else {
                        schemaUri = URIUtil.fromString(schemaFile);                    
                    }
                    this.fixedSchema = getSchemaFor(URIUtil.toURL(schemaUri));
                } catch (IOException e) {
                    throw new MorphlineCompilationException("Cannot parse external Avro schema file: " + schemaFile, config, e);
                } catch (java.net.URISyntaxException use) {
                    throw new MorphlineCompilationException("Cannot parse external Avro schema file: " + schemaFile, config, use);                    
                }
            } else {
                this.fixedSchema = null;
            }
            
            this.valueField = getConfigs().getString(config, "valueField", null);
            this.characterSet = Charset.forName(getConfigs().getString(config, "characterSet", StandardCharsets.UTF_8.name()));
            
            Config mappingsConfig = getConfigs().getConfig(config, "mappings", ConfigFactory.empty());
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(mappingsConfig)) {
                mappings.put(entry.getKey(), entry.getValue().toString());
            }
            
            Config propsConfig = getConfigs().getConfig(config, "props", ConfigFactory.empty());
            Properties props = new Properties();
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(propsConfig)) {
                props.put(entry.getKey().replaceAll("-", "."), entry.getValue());
            }
            
            String bootstrapServers = getConfigs().getString(propsConfig, "bootstrap-servers", null);
            if (bootstrapServers == null) {
                throw new MorphlineCompilationException(
                        "producer bootstrap-servers must be defined", config);
            }
            String schemaRegistryUrl = getConfigs().getString(propsConfig, "schema-registry-url", null);
            if (schemaRegistryUrl == null) {
                throw new MorphlineCompilationException(
                        "producer schema-registry-url must be defined", config);
            }
            
            validateArguments();
            
            if (!props.containsKey("acks")) props.put("acks", "all");
            if (!props.containsKey("retries")) props.put("retries", 0);
            if (!props.containsKey("key.serializer")) props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            if (!props.containsKey("value.serializer")) props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            this.producer = new MorphlineProducer(props);
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
                partition = Integer.valueOf((String) inputRecord.getFirstValue(topicField));
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
            org.apache.avro.Schema avroSchema;
            if (schemaField != null) {
                avroSchema = AVRO_CONVERTER.fromConnectSchema((org.apache.kafka.connect.data.Schema) inputRecord.getFirstValue(schemaField));
                Preconditions.checkNotNull(avroSchema);
            } else {
                avroSchema = fixedSchema;
            }
            log.debug("producer avroSchema: " + avroSchema);
            Object value = inputRecord.getFirstValue(valueField != null ? valueField : Fields.ATTACHMENT_BODY);
            log.debug("producer value: " + value);
            //disable for now till test framework catches up
            //producer.publish(topic, partition, timestamp, key, avroSchema, value);
            producer.close();
            return super.doProcess(inputRecord);
        }
        
        private static org.apache.avro.Schema getSchemaFor(String str) {
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            org.apache.avro.Schema schema = parser.parse(str);
            return schema;
          }

        private static org.apache.avro.Schema getSchemaFor(File file) {
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            org.apache.avro.Schema schema;
            try {
              schema = parser.parse(file);
            } catch (IOException e) {
              throw new RuntimeException("Failed to parse Avro schema from " + file.getName(), e);
            }
            return schema;
          }

          private static org.apache.avro.Schema getSchemaFor(InputStream stream) {
              org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
              org.apache.avro.Schema schema;
            try {
              schema = parser.parse(stream);
            } catch (IOException e) {
              throw new RuntimeException("Failed to parse Avro schema", e);
            }
            return schema;
          }

          private static org.apache.avro.Schema getSchemaFor(URL url) {
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
    
}