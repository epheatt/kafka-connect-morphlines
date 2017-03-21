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
import java.util.Map;

public class MorphlineSinkTask<T extends MorphlineSinkConnectorConfig> extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(MorphlineSinkTask.class);
  
  private static final Converter JSON_CONVERTER;
  static {
    JSON_CONVERTER = new JsonConverter();
    JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }
  
  protected MorphlineSinkConnectorConfig config;

  private MorphlineContext morphlineContext;
  private Command morphline;
  private Command finalChild;
  private String morphlineFileAndId;

  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  
  protected MorphlineSinkConnectorConfig config(Map<String, String> settings) {
      return new MorphlineSinkConnectorConfig(settings);
  }
  
//For test injection
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
      throw new MorphlineCompilationException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
    }
    morphlineFileAndId = morphlineFilePath + "@" + morphlineId;
    log.debug("Running: " + morphlineFileAndId);
    
    if (morphlineContext == null) {
      morphlineContext = new MorphlineContext.Builder().build();
    }
    Config morphlineFileConfig = ConfigFactory.empty();
    
    if (morphlineFilePath.startsWith("file:")) {
        morphlineFileConfig = ConfigFactory.parseFile(new File(morphlineFilePath.substring(morphlineFilePath.indexOf(":")+1)));
    } else if (morphlineFilePath.startsWith("url:")) {
        try {
            morphlineFileConfig = ConfigFactory.parseURL(new URL(morphlineFilePath.substring(morphlineFilePath.indexOf(":")+1)));
        } catch (java.net.MalformedURLException mue) {
            
        }
    } else if (morphlineFilePath.startsWith("resource:")) {
        morphlineFileConfig = ConfigFactory.parseResources(getClass(),morphlineFilePath.substring(morphlineFilePath.indexOf(":")+1));
    } else if (morphlineFilePath.startsWith("include ")) {
        morphlineFileConfig = ConfigFactory.parseString(morphlineFilePath);
    } else {
        morphlineFileConfig = ConfigFactory.parseResources(getClass(),morphlineFilePath);
    }
    if (morphlineFileConfig.isEmpty()) {
        throw new MorphlineCompilationException("Invalid content from parameter: " + MORPHLINE_FILE_PARAM, null);
    }
    log.debug("MorphlineFileConfig Content: " + morphlineFileConfig);
    Config override = ConfigFactory.parseMap(settings);
    log.debug("Overide Settings for Task: " + override);
    try {
        File morphlineFile = File.createTempFile("morphline", "."+morphlineId);
        morphlineFile.deleteOnExit();
        BufferedWriter out = new BufferedWriter(new FileWriter(morphlineFile));
        out.write(morphlineFileConfig.root().render());
        out.close();
        morphline = new Compiler().compile(morphlineFile, morphlineId, morphlineContext, finalChild, override.getConfig("morphlines"));
    } catch (java.io.IOException ioe) {
        throw new MorphlineCompilationException("Unable to compile morphline pipeline from: " + MORPHLINE_FILE_PARAM, null);
    }
  }
  
  @Override
  public void put(Collection<SinkRecord> collection) {
      // process each input data file
      //Notifications.notifyBeginTransaction(morphline);
      for (SinkRecord sinkRecord : collection) {
        Record record = new Record();
        record.put("kafkaTopic", sinkRecord.topic());
        record.put("kafkaPartition", sinkRecord.kafkaPartition());
        Object value = sinkRecord.value();
        Schema schema = sinkRecord.valueSchema();
        log.debug("SinkRecord value: " + value);
        log.debug("SinkRecord schema: " + schema);
        record.put("kafkaKey", sinkRecord.key());
        record.put("kafkaKeySchema", sinkRecord.keySchema());
        record.put("kafkaValue", value);
        record.put("kafkaValueSchema", schema);
        if (value instanceof String) {
            //try {
                record.put(Fields.ATTACHMENT_BODY, ((String) value).getBytes(StandardCharsets.UTF_8));
                record.put(Fields.ATTACHMENT_CHARSET, StandardCharsets.UTF_8);
            //} catch (java.io.UnsupportedEncodingException uee) {
            //   ; // ignore for now 
            //}
        } else {
            final String payload = new String(JSON_CONVERTER.fromConnectData(sinkRecord.topic(), schema, value), StandardCharsets.UTF_8);
            record.put(Fields.ATTACHMENT_BODY, payload.getBytes(StandardCharsets.UTF_8));
            record.put(Fields.ATTACHMENT_CHARSET, StandardCharsets.UTF_8);
        }
        //Notifications.notifyStartSession(morphline);
        if (!morphline.process(record)) {
            //Notifications.notifyRollbackTransaction(morphline);
        }
      }
      //Notifications.notifyCommitTransaction(morphline);
  }

  @Override
  public void stop() {
      //Notifications.notifyShutdown(morphline);
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

}
