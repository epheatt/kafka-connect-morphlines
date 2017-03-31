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


import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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

import java.nio.charset.StandardCharsets;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MorphlineSinkTask<T extends MorphlineSinkConnectorConfig> extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MorphlineSinkTask.class);

    protected MorphlineSinkConnectorConfig config;
    private MorphlineContext morphlineContext;
    private Command morphline;
    private Command finalChild;
    private String morphlineFileAndId;

    protected MorphlineSinkConnectorConfig config(Map<String, String> settings) {
        return new MorphlineSinkConnectorConfig(settings);
    }

    // For test injection
    void setMorphlineContext(MorphlineContext morphlineContext) {
        this.morphlineContext = morphlineContext;
    }

    // for interceptor
    void setFinalChild(Command finalChild) {
        this.finalChild = finalChild;
    }

    @Override
    public void start(Map<String, String> settings) {
        log.debug("Starting");
        this.config = config(settings);
        String morphlineFilePath = this.config.morphlineFile;
        String morphlineId = this.config.morphlineId;

        if (morphlineFilePath == null || morphlineFilePath.trim().length() == 0) {
            throw new MorphlineCompilationException("Missing parameter: " + MorphlineUtils.MORPHLINE_FILE_PARAM, null);
        }
        morphlineFileAndId = morphlineFilePath + "@" + morphlineId;
        log.debug("Running: " + morphlineFileAndId);

        if (morphlineContext == null) {
            morphlineContext = new MorphlineContext.Builder().build();
        }
        Config override = ConfigFactory.parseMap(getByPrefix(settings, "morphlines")).getConfig("morphlines");
        if (finalChild == null && override.hasPath("topic") && override.getString("topic") != null) {
            finalChild = new FinalCollector(override);
        }
        log.debug("Overide Settings for Morphlines Task: " + override);
        
        morphline = MorphlineUtils.compile(getClass(), morphlineFilePath, morphlineId, morphlineContext, finalChild, override);
        
    }
    
    private static HashMap<String, String> getByPrefix(Map<String, String> myMap, String prefix) {
        HashMap<String, String> local = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : myMap.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix))
                local.put(key, entry.getValue());
        }
        return local;
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        // process each input data file
        Notifications.notifyBeginTransaction(morphline);
        for (SinkRecord sinkRecord : collection) {
            Record record = MorphlineUtils.fromConnectData(sinkRecord);
            // Notifications.notifyStartSession(morphline);
            if (!morphline.process(record)) {
                log.warn("Record process failed sinkRecord: " + sinkRecord + " record:" + record);
                // Notifications.notifyRollbackTransaction(morphline);
            } else if (finalChild != null && finalChild instanceof FinalCollector) {
                log.info("Record process completed collector records:" + (((FinalCollector) finalChild).getRecords().get(0).getFirstValue("value")).equals(sinkRecord.value()) + " sink: " + sinkRecord.value() );
                //When there is configured morphlines.topic and as a result finalCollector 
                //send the resulting record(s) to that topic if a morhphlines.bootstrap broker list
                //is available to attach a producer to send via.
                ((FinalCollector) finalChild).reset();
            }
        }
        Notifications.notifyCommitTransaction(morphline);
    }

    @Override
    public void stop() {
        if (morphline != null)
            Notifications.notifyShutdown(morphline);
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

}
