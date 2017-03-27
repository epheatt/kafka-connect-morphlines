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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class MorphlineSinkTaskTest {
    private static final Logger log = LoggerFactory.getLogger(MorphlineSinkTaskTest.class);
    MorphlineSinkTask task;
    SolrClient client;

    @BeforeEach
    public void before() throws IOException, SolrServerException {
        this.task = mock(MorphlineSinkTask.class, Mockito.CALLS_REAL_METHODS);
        this.client = mock(SolrClient.class);
        // when(this.task.client()).thenReturn(this.client);
        when(this.task.config(anyMap())).thenAnswer(invocationOnMock -> {
            Map<String, String> settings = invocationOnMock.getArgument(0);
            return new MorphlineSinkConnectorConfig(settings);
        });
        when(this.client.request(any(UpdateRequest.class), any())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                log.trace("request");
                NamedList<Object> result = new NamedList<>();
                NamedList<Object> responseHeaders = new NamedList<>();
                responseHeaders.add("status", 200);
                responseHeaders.add("QTime", 123);
                result.add("responseHeader", responseHeaders);
                return result;
            }
        });
    }

    @Test
    public void testLoadSolr() {
        Map<String, String> settings = ImmutableMap.of(
                "morphlines.morphlineFile", "resource:httpsolr.conf", 
                "morphlines.morphlineId", "httpsolr",
                "morphlines.solrUrl", "http://localhost:9231", 
                "morphlines.collection", "muffins"
            );
        this.task.start(settings);
        List<SinkRecord> records = Records.records();
        this.task.put(records);
    }

    @Test
    public void testReadJson() {
        Map<String, String> settings = ImmutableMap.of(
                "morphlines.morphlineFile", "resource:readjson.conf", 
                "morphlines.morphlineId", "readjson"
            );
        this.task.start(settings);
        List<SinkRecord> records = Records.records();
        this.task.put(records);
    }

    @Test
    public void testReadLine() {
        Map<String, String> settings = ImmutableMap.of(
                "morphlines.morphlineFile", "resource:readline.conf", 
                "morphlines.morphlineId", "readline"
            );
        this.task.start(settings);
        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(Records.string().record);
        this.task.put(records);
    }

}
