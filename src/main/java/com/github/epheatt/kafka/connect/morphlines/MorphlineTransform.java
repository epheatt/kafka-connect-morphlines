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

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MorphlineTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(MorphlineTransform.class);
    
    private static final String MORPHLINE_FILE_PARAM = "morphlineFile";
    private static final String MORPHLINE_ID_PARAM = "morphlineId";
    
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
        
        morphline = MorphlineUtils.compile(getClass(), morphlineFile, morphlineId, morphlineContext, finalChild, override);
    }
    
    @Override
    public R apply(R record) {
        log.debug("Apply: " + morphlineFileAndId);
        //pass through no-op for now
        Record connectRecord = MorphlineUtils.fromConnectData(record);
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
        //send the resulting records from the FinalCollector command unless a dropRecord was issued
        ((FinalCollector) finalChild).reset();
        Notifications.notifyCommitTransaction(morphline);
        return record;
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