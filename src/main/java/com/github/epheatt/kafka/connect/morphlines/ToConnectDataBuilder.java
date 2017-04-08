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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public final class ToConnectDataBuilder implements CommandBuilder {
    private static final Logger log = LoggerFactory.getLogger(ToConnectDataBuilder.class);
    
    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("toConnectData");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ToConnectData(this, config, parent, child, context);
    }

    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    /** Implementation that does logging and metrics */
    private static final class ToConnectData extends AbstractCommand {
        private static final Logger log = LoggerFactory.getLogger(ToConnectData.class);

        private final Map<String, String> mappings = new HashMap<String, String>();
        private final String topicField;
        private final String schemaField;
        private final String valueField;
        private final Charset characterSet;
        private static final AvroData AVRO_CONVERTER;
        static {
            AVRO_CONVERTER = new AvroData(new AvroDataConfig.Builder()
                                            //.with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize)
                                            .build());
        }
        
        public ToConnectData(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);

            this.topicField = getConfigs().getString(config, "topicField", "_topic");
            this.schemaField = getConfigs().getString(config, "schemaField", null);
            
            int numDefinitions = 0;
            if (schemaField != null) {
              numDefinitions++;
            }
            if (numDefinitions == 0) {
              throw new MorphlineCompilationException(
                "Either schemaFile or schemaString or schemaField must be defined", config);
            }
            
            this.valueField = getConfigs().getString(config, "valueField", "_value");
            this.characterSet = Charset.forName(getConfigs().getString(config, "characterSet", StandardCharsets.UTF_8.name()));
            
            Config mappingsConfig = getConfigs().getConfig(config, "mappings", ConfigFactory.empty());
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(mappingsConfig)) {
                mappings.put(entry.getKey(), entry.getValue().toString());
            }
            validateArguments();
        }

        @Override
        protected boolean doProcess(Record inputRecord) {
            Record outputRecord = inputRecord.copy();
            String topic = (String) inputRecord.getFirstValue(topicField);
            Schema schema = (Schema) inputRecord.getFirstValue(schemaField);
            //org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) inputRecord.getFirstValue(schemaField);
            if (schema == null) {
                //schema = AVRO_CONVERTER.toConnectSchema(avroSchema);
                //inputRecord.replaceValues("valueSchema", schema);
                HashMap<String, Object> valueM = new HashMap<String, Object>();
                for (Map.Entry<String, String> entry : mappings.entrySet()) {
                    List list = inputRecord.get(entry.getValue());
                    if (list.size() == 1) {
                        valueM.put(entry.getKey(), list.get(0));
                    } else if (list.size() == 1) {
                        valueM.put(entry.getKey(), list);
                    }
                }
                outputRecord.replaceValues(valueField, valueM);
            } else if (schema.type() == Schema.STRING_SCHEMA.type() ) {
                outputRecord.replaceValues(valueField, ((String) inputRecord.getFirstValue(Fields.ATTACHMENT_BODY)));
            } else {
                Struct valueS = new Struct(schema);
                for (Field field : schema.fields()) {
                    String morphlineFieldName = mappings.get(field.name());
                    if (morphlineFieldName == null) {
                      morphlineFieldName = field.name();
                    }
                    List list = inputRecord.get(morphlineFieldName);
                    if (list.size() == 1) {
                        valueS.put(field.name(), list.get(0));
                    } else if (list.size() > 1) {
                        valueS.put(field.name(), list);
                    }
                }
                outputRecord.replaceValues(valueField, valueS);
            }
            return super.doProcess(outputRecord);
            /*
            Object value = inputRecord.getFirstValue(Fields.ATTACHMENT_BODY);
            switch (converterType.toLowerCase()) {
                case "string":
                    outputRecord.replaceValues(valueField, convertString(value));
                    break;
                case "json":
                    outputRecord.replaceValues(valueField, convertJson(topic, value));
                    break;
                case "avro":
                default:
                    outputRecord.replaceValues(valueField, convertAvro(avroSchema, value));
            }
            // pass record to next command in chain:
            return super.doProcess(outputRecord);
            */
        }
        
    }

}