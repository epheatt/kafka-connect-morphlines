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

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MorphlineTransformTest {
    private static final Logger log = LoggerFactory.getLogger(MorphlineTransformTest.class);

    @BeforeEach
    public void before() {
        System.setProperty("CONNECT_BOOTSTRAP_SERVERS", "localhost:9092");
        System.setProperty("CONNECT_SCHEMA_REGISTRY_URL", "http://localhost:8081");
        System.setProperty("CONNECT_KAFKA_REST_URL", "http://localhost:8082");
    }
    
    @Disabled
    @Test 
    void testNoOpHttp() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "https://raw.githubusercontent.com/kite-sdk/kite/738b397cf1f8b1fbea4fbe9bda017839081d9f37/kite-morphlines/kite-morphlines-core/src/test/resources/test-morphlines/parseInclude.conf",
                "morphlineId", "morphline1"
            );
        xform.configure(settings);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("dont"));
    }
    
    @Disabled
    @Test 
    void testNoOpIncludeFile() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "include \"file(../../src/test/resources/com/github/epheatt/kafka/connect/morphlines/identity.conf)\"", 
                "morphlineId", "noop"
            );
        xform.configure(settings);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("dont"));
    }
    
    @Disabled
    @Test 
    void testNoOpRelativeFile() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "file:../../src/test/resources/com/github/epheatt/kafka/connect/morphlines/identity.conf", 
                "morphlineId", "noop"
            );
        xform.configure(settings);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("whatever", updatedValue.getString("dont"));
    }
    
    @Test 
    public void testNoOpResource() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();
        
        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "resource:identity.conf", 
                "morphlineId", "noop"
            );
        xform.configure(settings);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("abc"));
        assertEquals(true, updatedValue.getBoolean("foo"));
    }

    @Test
    public void testEnrichJson() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "resource:transform.conf",
                "morphlineId", "enrichjson"
        );
        xform.configure(settings);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final Schema xschema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.OPTIONAL_STRING_SCHEMA)
                .field("missing", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals("etc", updatedValue.getString("etc"));
        assertEquals(null, updatedValue.get("missing"));
    }

    @Test 
    public void testDrop() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();
        
        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "resource:transform.conf", 
                "morphlineId", "drop"
            );
        xform.configure(settings);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);
        
        assertEquals(null, transformedRecord);
    }
    
    @Test
    public void testReadJson() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "resource:identity.conf", 
                "morphlineId", "readjson"
            );
        xform.configure(settings);

        final SinkRecord transformedRecord = xform.apply(Records.map().record);

        final Map updatedValue = (Map) transformedRecord.value();

        assertEquals(4, updatedValue.size());
        assertEquals("example", updatedValue.get("firstName"));
        assertEquals("user", updatedValue.get("lastName"));
        assertEquals("example.user@example.com", updatedValue.get("email"));
        assertEquals(27, updatedValue.get("age"));
    }

    @Test
    public void testReadAvro() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "resource:identity.conf", 
                "morphlineId", "readavro"
            );
        xform.configure(settings);

        final SinkRecord transformedRecord = xform.apply(Records.struct().record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals("example", updatedValue.getString("firstName"));
        assertEquals("user", updatedValue.getString("lastName"));
        assertEquals("example.user@example.com", updatedValue.getString("email"));
        assertEquals(new Integer(27), updatedValue.getInt32("age"));
    }

    @Test
    public void testReadLine() {
        final MorphlineTransform<SinkRecord> xform = new MorphlineTransform<>();

        Map<String, String> settings = ImmutableMap.of(
                "morphlineFile", "resource:identity.conf", 
                "morphlineId", "readline"
            );
        xform.configure(settings);

        final SinkRecord transformedRecord = xform.apply(Records.string().record);

        final String updatedValue = (String) transformedRecord.value();
        
        assertEquals("{\"firstName\":\"example\",\"lastName\":\"user\",\"email\":\"example.user@example.com\",\"age\":27}", updatedValue);
    }
    
}
