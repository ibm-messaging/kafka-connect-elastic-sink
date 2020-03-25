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
package com.ibm.eventstreams.connect.essink.builders;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds JSON messages from Kafka Connect SinkRecords.
 */
public class BaseDocumentBuilder implements DocumentBuilder {
  private static final String classname = BaseDocumentBuilder.class.getName();
  private static final Logger log = LoggerFactory.getLogger(BaseDocumentBuilder.class);

  static Base64.Encoder encoder = Base64.getEncoder();

  public String topicPropertyName;
  public String partitionPropertyName;
  public String offsetPropertyName;

  /**
   * Configure this class.
   *
   * @param props initial configuration
   *
   * @throws ConnectException   Operation failed and connector should stop.
   */
  @Override
  public void configure(Map<String, String> props) {
    log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), classname, props);

    log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), classname);
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
    JSONObject jsonObject = new JSONObject();

    Schema schema = record.valueSchema();
    Object key = record.key();
    Object value = record.value();
    String body = null;

    log.trace("[{}] Entry {}.fromSinkRecord", Thread.currentThread().getId(), classname);

    if (schema != null)
      jsonObject.put("schema", schema.name());
    if (key != null)
      jsonObject.put("key", key.toString());

    if (schema == null) {
      if (value instanceof byte[]) {
        body = encoder.encodeToString((byte[]) value);
      }
      else if (value instanceof ByteBuffer) {
        body = encoder.encodeToString(((ByteBuffer) value).array());
      } else if (value != null) {
        body = value.toString();
      }
    }
    else if (schema.type() == Type.BYTES) {
      if (value instanceof byte[]) {
        body = encoder.encodeToString((byte[]) value);
      }
      else if (value instanceof ByteBuffer) {
        body = encoder.encodeToString(((ByteBuffer) value).array());
      }
    }
    else {
      if (value != null) {
        body = value.toString();
      }
    }

    if (body != null) {
      jsonObject.put("body", body);
    }

    log.trace("[{}] Exit {}.fromSinkRecord", Thread.currentThread().getId(), classname);

    return jsonObject.toString();
  }
}
