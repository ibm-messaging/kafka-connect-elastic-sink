/**
 * Copyright 2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.elasticsink.builders;

import java.util.HashMap;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.*;

/**
 * Builds JSON documents from Kafka Connect SinkRecords
 */
public class JsonDocumentBuilder implements DocumentBuilder {
    private static final String classname = JsonDocumentBuilder.class.getName();
    private static final Logger log = LoggerFactory.getLogger(JsonDocumentBuilder.class);

    private JsonConverter converter;

    public JsonDocumentBuilder() {
        log.info("Building documents using {}", classname);
        converter = new JsonConverter();
        
        // We just want the payload, not the schema in the output message
        HashMap<String, String> m = new HashMap<>();
        m.put("schemas.enable", "false");

        // Convert the value, not the key (isKey == false)
        converter.configure(m, false);
    }

    /**
     * Convert a Kafka Connect SinkRecord into a message.
     *
     * @param record             the Kafka Connect SinkRecord
     *
     * @return the message
     */
    @Override
    public String fromSinkRecord(SinkRecord record) {
        log.trace("[{}] Entry {}.fromSinkRecord", Thread.currentThread().getId(), classname);

        byte[] payload = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String document = null;
        if (payload != null) {
            document = new String(payload, UTF_8);
        }

        log.trace("[{}] Exit {}.fromSinkRecord", Thread.currentThread().getId(), classname);
        return document;
    }
}
