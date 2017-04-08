package com.github.epheatt.kafka.connect.morphlines;


import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
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

public final class FromConnectDataBuilder implements CommandBuilder {
    private static final Logger log = LoggerFactory.getLogger(FromConnectDataBuilder.class);

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("fromConnectData");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new FromConnectData(this, config, parent, child, context);
    }

    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    /** Implementation that does logging and metrics */
    private static final class FromConnectData extends AbstractCommand {
        private static final Logger log = LoggerFactory.getLogger(FromConnectData.class);
        
        private final String topicField;
        private final String schemaField;
        private final String valueField;
        private final String converterType;
        private final Charset characterSet;
        private static final Converter JSON_CONVERTER;
        static {
            JSON_CONVERTER = new JsonConverter();
            JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
        }

        private static final AvroData AVRO_CONVERTER;
        static {
            AVRO_CONVERTER = new AvroData(new AvroDataConfig.Builder()
                                            //.with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize)
                                            .build());
        }
        
        public FromConnectData(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);

            this.topicField = getConfigs().getString(config, "topicField", "_topic");
            this.schemaField = getConfigs().getString(config, "schemaField", "_valueSchema");
            
            int numDefinitions = 0;
            if (schemaField != null) {
              numDefinitions++;
            }
            if (numDefinitions == 0) {
              throw new MorphlineCompilationException(
                "Either schemaFile or schemaString or schemaField must be defined", config);
            }
            
            this.valueField = getConfigs().getString(config, "valueField", "_value");
            this.converterType = getConfigs().getString(config, "converter", "avro");
            this.characterSet = Charset.forName(getConfigs().getString(config, "characterSet", StandardCharsets.UTF_8.name()));

            validateArguments();
        }

        @Override
        protected boolean doProcess(Record inputRecord) {
            Record outputRecord = inputRecord.copy();
            AbstractParser.removeAttachments(outputRecord);
            Schema schema = (Schema) inputRecord.getFirstValue(schemaField);
            if (schema == null) {
                //outputRecord.replaceValues("valueSchema", AVRO_CONVERTER.toConnectSchema(AVRO_CONVERTER.fromConnectSchema(schema)));
            }
            Object value = inputRecord.getFirstValue(valueField != null ? valueField : Fields.ATTACHMENT_BODY);
            switch (converterType.toLowerCase()) {
                case "string":
                    outputRecord.put(Fields.ATTACHMENT_BODY, ((String) value).getBytes(characterSet));
                    outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, "text/plain");
                    break;
                case "json":
                    final String payload = new String(JSON_CONVERTER.fromConnectData(topicField, schema, value), characterSet);
                    outputRecord.put(Fields.ATTACHMENT_BODY, payload.getBytes(characterSet));
                    outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, "application/json");
                    break;
                case "avro":
                default:
                    outputRecord.put(Fields.ATTACHMENT_BODY, AVRO_CONVERTER.fromConnectData(schema, value));
                    outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, "application/avro");
            }
            outputRecord.put(Fields.ATTACHMENT_CHARSET, characterSet.name()); 
            outputRecord.replaceValues("_valueSchemaAvro", AVRO_CONVERTER.fromConnectSchema(schema));
            // pass record to next command in chain:
            return super.doProcess(outputRecord);
        }

    }

}