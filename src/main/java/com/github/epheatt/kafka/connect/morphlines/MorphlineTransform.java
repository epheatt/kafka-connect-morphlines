/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.epheatt.kafka.connect.morphlines;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.net.URL;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MorphlineTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(MorphlineTransform.class);
    
    private static final Object LOCK = new Object();
    private static Compiler morphlineCompiler;
    static {
        morphlineCompiler = new Compiler();
    }
    
    public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
    public static final String MORPHLINE_ID_PARAM = "morphlineId";
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(MORPHLINE_FILE_PARAM, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Morphline File Path to include.")
        .define(MORPHLINE_ID_PARAM, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Morphline Id to process.");
    
    private String morphlineFile;
    private String morphlineId;
    private MorphlineContext morphlineContext;
    private Command morphline;
    private Command finalChild;
    private String morphlineFileAndId;
    
    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        morphlineFile = config.getString(MORPHLINE_FILE_PARAM);
        morphlineId = config.getString(MORPHLINE_ID_PARAM);

        if (morphlineFile == null || morphlineId == null) {
            throw new ConfigException("Neither morphline File nor Id configured");
        }
        
        morphlineFileAndId = morphlineFile + "@" + morphlineId;
        log.debug("Configure: " + morphlineFileAndId);

        if (morphlineContext == null) {
            morphlineContext = new MorphlineContext.Builder().build();
        }
        Config override = ConfigFactory.parseMap(configs);
        if (finalChild == null) {
            finalChild = new FinalCollector(override);
        }
        log.debug("Overide Settings for Morphlines Task: " + override);
        
        morphline = compile(getClass(), morphlineFile, morphlineId, morphlineContext, finalChild, override);
    }
    
    @Override
    public R apply(R record) {
        log.debug("Apply: " + morphlineFileAndId);
        //pass through no-op for now
        Record connectRecord = fromConnectData((ConnectRecord) record);
        Notifications.notifyBeginTransaction(morphline);
        if (!morphline.process(connectRecord)) {
            log.warn("Record process failed record: " + record + " connectRecord:" + connectRecord);
            Notifications.notifyRollbackTransaction(morphline);
            return null;
        } else if (finalChild == null || ((FinalCollector) finalChild).getRecords().size() == 0) {
            log.warn("Record process droped record: " + record + " connectRecord:" + connectRecord);
            Notifications.notifyRollbackTransaction(morphline);
            return null;
        }
        log.info("Record process completed collector record:" + ((FinalCollector) finalChild).getRecords());
        connectRecord = ((FinalCollector) finalChild).getRecords().get(0);
        //send the resulting records from the FinalCollector command unless a dropRecord was issued
        ((FinalCollector) finalChild).reset();
        Notifications.notifyCommitTransaction(morphline);
        return connectRecord == null ? null : (R) (toConnectData((ConnectRecord) record, connectRecord));
    }
    
    public static Record fromConnectData(ConnectRecord connectData) {
        String topic = connectData.topic();
        log.debug("Record Topic: " + topic);
        Object value = connectData.value();
        log.debug("Record Value: " + value);
        Schema schema = connectData.valueSchema();
        log.debug("Record Schema: " + schema);
        Record record = new Record();
        record.put("_topic", topic);
        record.put("_kafkaPartition", connectData.kafkaPartition());
        record.put("_key", connectData.key());
        record.put("_keySchema", connectData.keySchema());
        record.put("_value", value);
        record.put("_valueSchema", schema);
        if (connectData instanceof SinkRecord) {
            record.put("_kafkaOffset", ((SinkRecord) connectData).kafkaOffset());
            record.put("_timestamp", ((SinkRecord) connectData).timestamp());
            record.put("_timestampType", ((SinkRecord) connectData).timestampType());
        }
        return record;
    }
    
    public static ConnectRecord toConnectData(ConnectRecord connectData, Record record) {
        String topic = (String) record.getFirstValue("_topic");
        log.debug("Record Topic: " + topic);
        Integer kafkaPartition = (Integer) record.getFirstValue("_kafkaPartition");
        Schema keySchema = (Schema) record.getFirstValue("_keySchema");
        Object key = (Object) record.getFirstValue("_key");
        Schema valueSchema = (Schema) record.getFirstValue("_valueSchema");
        log.debug("Record Schema: " + valueSchema);
        Object value = (Object) record.getFirstValue("_value");
        log.debug("Record Value: " + value);
        Long timestamp = (Long) record.getFirstValue("_timestamp");
        return connectData.newRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp);
    }
    
    public static Command compile(Class clazz, String morphlineFilePath, String morphlineId, MorphlineContext morphlineContext, Command finalChild, Config override) throws MorphlineCompilationException {
        Config morphlineFileConfig = ConfigFactory.empty();

        if (morphlineFilePath.startsWith("url:")) {
            try {
                morphlineFileConfig = ConfigFactory.parseURL(new URL(morphlineFilePath.substring(morphlineFilePath.indexOf(":") + 1)));
            } catch (java.net.MalformedURLException mue) {

            }
        } else if (morphlineFilePath.startsWith("resource:")) {
            morphlineFileConfig = ConfigFactory.parseResources(clazz, morphlineFilePath.substring(morphlineFilePath.indexOf(":") + 1));
        } else if (morphlineFilePath.startsWith("include ")) {//TODO: broken for now need tests
            morphlineFileConfig = ConfigFactory.parseString(morphlineFilePath);
        } else if (morphlineFilePath.startsWith("file:")) {
            morphlineFileConfig = ConfigFactory.parseFile(new File(morphlineFilePath.substring(morphlineFilePath.indexOf(":") + 1)));
        } else {
            morphlineFileConfig = ConfigFactory.parseFile(new File(morphlineFilePath));
        }
        if (morphlineFileConfig.isEmpty()) {
            throw new MorphlineCompilationException("Invalid content from parameter: " + MORPHLINE_FILE_PARAM, null);
        }
        log.debug("MorphlineFileConfig Content: " + morphlineFileConfig);
        Config config = override.withFallback(morphlineFileConfig);
        synchronized (LOCK) {
            ConfigFactory.invalidateCaches();
            config = ConfigFactory.load(config);
            config.checkValid(ConfigFactory.defaultReference()); // eagerly validate aspects of tree config
        }
        Config morphlineConfig = morphlineCompiler.find(morphlineId, config, morphlineFilePath);
        return morphlineCompiler.compile(morphlineConfig, morphlineContext, finalChild);
    }
    
    @Override
    public void close() {
        //schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends MorphlineTransform<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends MorphlineTransform<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}