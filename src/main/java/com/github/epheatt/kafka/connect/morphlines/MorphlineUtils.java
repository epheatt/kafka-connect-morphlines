/**
 * Copyright © 2017 Eric Pheatt (eric.pheatt@gmail.com)
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


import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphlineUtils {
    private static final Logger log = LoggerFactory.getLogger(MorphlineUtils.class);

    private static final Object LOCK = new Object();
    private static Compiler morphlineCompiler;
    static {
        morphlineCompiler = new Compiler();
    }
    
    private static final Converter JSON_CONVERTER;
    static {
        JSON_CONVERTER = new JsonConverter();
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
    public static final String MORPHLINE_ID_PARAM = "morphlineId";
    /** The MIME type identifier that will be filled into output records */
    public static final String CONNECT_MEMORY_MIME_TYPE = "connectrecord/java+memory";
    
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
    
    public static Record fromConnectData(ConnectRecord connectData) {
        String topic = connectData.topic();
        log.debug("Record Topic: " + topic);
        Object value = connectData.value();
        log.debug("Record Value: " + value);
        Schema schema = connectData.valueSchema();
        log.debug("Record Schema: " + schema);
        Record record = new Record();
        record.put("topic", topic);
        record.put("kafkaPartition", connectData.kafkaPartition());
        if (connectData instanceof SinkRecord) {
            record.put("kafkaOffset", ((SinkRecord) connectData).kafkaOffset());
            record.put("timestamp", ((SinkRecord) connectData).timestamp());
            record.put("timestampType", ((SinkRecord) connectData).timestampType());
        }
        record.put("key", connectData.key());
        record.put("keySchema", connectData.keySchema());
        record.put("value", value);
        record.put("valueSchema", schema);
        if (schema != null && schema.type() == Schema.Type.STRING) {
            record.put(Fields.ATTACHMENT_BODY, ((String) value).getBytes(StandardCharsets.UTF_8));
            record.put(Fields.ATTACHMENT_MIME_TYPE, "text/plain");
        } else {
            final String payload = new String(JSON_CONVERTER.fromConnectData(topic, schema, value), StandardCharsets.UTF_8);
            record.put(Fields.ATTACHMENT_BODY, payload.getBytes(StandardCharsets.UTF_8));
            record.put(Fields.ATTACHMENT_MIME_TYPE, "application/json");
        }
        record.put(Fields.ATTACHMENT_CHARSET, StandardCharsets.UTF_8);
        return record;
    }
    /*
    public static ConnectRecord toConnectData(Record record) {
        String topic = (String) record.getFirstValue("topic");
        Integer kafkaPartition = (Integer) record.getFirstValue("kafkaPartition");
        Schema keySchema = (Schema) record.getFirstValue("keySchema");
        Object key = record.getFirstValue("key");
        Schema valueSchema = (Schema) record.getFirstValue("valueSchema");
        Object value = record.getFirstValue("keySchema");
        Long timestamp = (Long) record.getFirstValue("timestamp");
        ConnectRecord connectRecord = new ConnectRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp) ;
        return connectRecord;
    }
    */
    
}