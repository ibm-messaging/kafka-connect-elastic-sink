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

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the name of the index into which the event is inserted
 */
public class BaseIndexBuilder implements IndexBuilder {
  private static final String classname = BaseIndexBuilder.class.getName();
  private static final Logger log = LoggerFactory.getLogger(BaseIndexBuilder.class);

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
   * This implementation is very simple. It just converts
   * the topic associated with the record into lowercase.
   * 
   * The transform ensures that there are no '/' characters
   * in the topic, which are not valid in Kafka anyway, but
   * this makes it doubly certain.
   *
   * @param record             the Kafka Connect SinkRecord
   *
   * @return the message
   */
  @Override
  public String generateIndex(SinkRecord record) {
    
    log.trace("[{}] Entry {}.generateIndex", Thread.currentThread().getId(), classname);

    String index = record.topic().replaceAll("/","!").toLowerCase();

    log.trace("[{}] Exit {}.generateIndex", Thread.currentThread().getId(), classname);

    return index;
  }
}
