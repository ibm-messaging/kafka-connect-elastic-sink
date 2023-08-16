/**
 * Copyright 2020, 2023 IBM Corporation
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

import java.util.Locale;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the name of the index into which the event is inserted
 */
public class DefaultIndexBuilder implements IndexBuilder {
    private static final String CLASSNAME = DefaultIndexBuilder.class.getName();
    private static final Logger log = LoggerFactory.getLogger(DefaultIndexBuilder.class);

    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    @Override
    public void configure(final Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), CLASSNAME, props);
        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), CLASSNAME);
    }

    /**
     * Convert a Kafka Connect SinkRecord into a message. This implementation is
     * very simple. It just converts the topic associated with the record into
     * lowercase.
     *
     * <p>The index name must be:
     * <ul>
     * <li> lowercase
     * <li> no /,\,*,?,",<,>,|,#,:, or comma
     * <li> cannot start with -,_,+
     * <li> cannot be . or ..
     * <li> maximum length of 255 bytes
     * </ul>
     *
     * The Kafka topic name can be:
     * <ul>
     * <li> a-z,A-Z,0-9,.,_.-
     * <li> maximum length of 249 characters
     * </ul>
     *
     * @param record the Kafka Connect SinkRecord
     *
     * @return the message
     */
    @Override
    public String generateIndex(final SinkRecord record) {
        log.trace("[{}] Entry {}.generateIndex", Thread.currentThread().getId(), CLASSNAME);

        final String index = record.topic().toLowerCase(Locale.ENGLISH);

        if (index.equals(".") || index.equals("..") || index.startsWith("-") || index.startsWith("_")) {
            throw new ConnectException("Invalid index name " + index + " for topic name " + record.topic());
        }

        log.trace("[{}] Exit {}.generateIndex", Thread.currentThread().getId(), CLASSNAME);

        return index;
    }
}
