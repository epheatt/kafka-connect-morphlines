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


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class MorphlineProducer {
    private static final Logger log = LoggerFactory.getLogger(MorphlineProducer.class);
    
    private Producer<String, Object> producer;
    private Properties settings;
    
    public MorphlineProducer(Properties props) {
        this.settings = props;
        this.producer = new KafkaProducer<String, Object>(settings);
    }
    
    public void publish(String topic, Integer partition, Long timestamp, Object key, Schema schema, Object value) {
        producer.send(new ProducerRecord(topic, partition, timestamp, key, value));
    }
    
    public void close() {
        producer.close();
    }
}