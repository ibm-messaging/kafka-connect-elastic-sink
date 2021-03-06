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

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Creates the name of the Elasticsearch index from Kafka Connect SinkRecords
 */
public interface IndexBuilder {
    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    default void configure(Map<String, String> props) {}

    /**
     * Generate the index name from a Kafka Connect SinkRecord.
     *
     * @param record             the Kafka Connect SinkRecord
     *
     * @return the index
     */
    String generateIndex(SinkRecord record);
}
