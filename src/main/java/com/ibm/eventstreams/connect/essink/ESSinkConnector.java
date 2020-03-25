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
package com.ibm.eventstreams.connect.essink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESSinkConnector extends SinkConnector {
  private static final String classname = ESSinkConnector.class.getName();

  private static final Logger log = LoggerFactory.getLogger(ESSinkConnector.class);

  public static final String CONFIG_GROUP_ES = "es";

  // Resource connection information
  public static final String CONFIG_NAME_ES_CONNECTION = "es.connection";
  public static final String CONFIG_DOCUMENTATION_ES_CONNECTION = "The hostname:port to connect to Elasticsearch.";
  public static final String CONFIG_DISPLAY_ES_CONNECTION = "hostname:port";

  public static final String CONFIG_NAME_ES_USER_NAME = "es.user.name";
  public static final String CONFIG_DOCUMENTATION_ES_USER_NAME = "The user name for authenticating with Elasticsearch.";
  public static final String CONFIG_DISPLAY_ES_USER_NAME = "User name";

  public static final String CONFIG_NAME_ES_PASSWORD = "es.password";
  public static final String CONFIG_DOCUMENTATION_ES_PASSWORD = "The password for authenticating with Elasticsearch.";
  public static final String CONFIG_DISPLAY_ES_PASSWORD = "Password";

  public static final String CONFIG_NAME_ES_TLS_KEYSTORE_LOCATION = "es.tls.keystore.location";
  public static final String CONFIG_DOCUMENTATION_ES_TLS_KEYSTORE_LOCATION = "The path to the JKS keystore to use for the TLS connection.";
  public static final String CONFIG_DISPLAY_ES_TLS_KEYSTORE_LOCATION = "TLS keystore location";

  public static final String CONFIG_NAME_ES_TLS_KEYSTORE_PASSWORD = "es.tls.keystore.password";
  public static final String CONFIG_DOCUMENTATION_ES_TLS_KEYSTORE_PASSWORD = "The password of the JKS keystore to use for the TLS connection.";
  public static final String CONFIG_DISPLAY_ES_TLS_KEYSTORE_PASSWORD = "TLS keystore password";

  public static final String CONFIG_NAME_ES_TLS_TRUSTSTORE_LOCATION = "es.tls.truststore.location";
  public static final String CONFIG_DOCUMENTATION_ES_TLS_TRUSTSTORE_LOCATION = "The path to the JKS truststore to use for the TLS connection.";
  public static final String CONFIG_DISPLAY_ES_TLS_TRUSTSTORE_LOCATION = "TLS truststore location";

  public static final String CONFIG_NAME_ES_TLS_TRUSTSTORE_PASSWORD = "es.tls.truststore.password";
  public static final String CONFIG_DOCUMENTATION_ES_TLS_TRUSTSTORE_PASSWORD = "The password of the JKS truststore to use for the TLS connection.";
  public static final String CONFIG_DISPLAY_ES_TLS_TRUSTSTORE_PASSWORD = "TLS truststore password";

  public static final String CONFIG_NAME_ES_HTTP_PROXY_HOST = "es.http.proxy.host";
  public static final String CONFIG_DOCUMENTATION_ES_HTTP_PROXY_HOST = "Hostname for HTTP proxy.";
  public static final String CONFIG_DISPLAY_ES_HTTP_PROXY_HOST = "HTTP Proxy host";

  public static final int DEFAULT_HTTP_PROXY_PORT = 8080;
  public static final String CONFIG_NAME_ES_HTTP_PROXY_PORT = "es.http.proxy.port";
  public static final String CONFIG_DOCUMENTATION_ES_HTTP_PROXY_PORT = "Port number for HTTP proxy.";
  public static final String CONFIG_DISPLAY_ES_HTTP_PROXY_PORT = "HTTP Proxy port number";

  // Message reformatting attributes
  public static final String CONFIG_NAME_ES_DOCUMENT_BUILDER = "es.document.builder";
  public static final String CONFIG_DOCUMENTATION_ES_DOCUMENT_BUILDER = "The class used to build the document content.";
  public static final String CONFIG_DISPLAY_ES_DOCUMENT_BUILDER = "Document builder";

  // Jetty configuration options
  public static final int DEFAULT_JETTY_MAX_CONNECTIONS = 5;
  public static final int DEFAULT_JETTY_IDLE_TIMEOUT_SEC = 30;
  public static final int DEFAULT_JETTY_CONNECTION_TIMEOUT_SEC = 10;
  public static final int DEFAULT_JETTY_OPERATION_TIMEOUT_SEC = 6; // This is the one you will most likely want to change
  public static final int DEFAULT_MAX_COMMIT_FAILURES = 5;

  public static final String CONFIG_NAME_ES_JETTY_CONNECTIONS = "es.http.connections";
  public static final String CONFIG_DOCUMENTATION_ES_JETTY_CONNECTIONS = "The maximum number of HTTP connections to Elasticsearch";
  public static final String CONFIG_DISPLAY_ES_JETTY_CONNECTIONS = "HTTP connections to Elasticsearch";

  public static final String CONFIG_NAME_ES_JETTY_IDLE_TIMEOUT_SEC = "es.http.timeout.idle";
  public static final String CONFIG_DOCUMENTATION_ES_JETTY_IDLE_TIMEOUT_SEC = "Timeout for idle HTTP connections to Elasticsearch";
  public static final String CONFIG_DISPLAY_ES_JETTY_IDLE_TIMEOUT_SEC = "Timeout for idle HTTP connections to Elasticsearch";

  public static final String CONFIG_NAME_ES_JETTY_CONNECTION_TIMEOUT_SEC = "es.http.timeout.connection";
  public static final String CONFIG_DOCUMENTATION_ES_JETTY_CONNECTION_TIMEOUT_SEC = "Time (seconds) allowed for initial HTTP connection to Elasticsearch";
  public static final String CONFIG_DISPLAY_ES_JETTY_CONNECTION_TIMEOUT_SEC = "Time (seconds) allowed for initial HTTP connection to Elasticsearch";

  public static final String CONFIG_NAME_ES_JETTY_OPERATION_TIMEOUT_SEC = "es.http.timeout.operation";
  public static final String CONFIG_DOCUMENTATION_ES_JETTY_OPERATION_TIMEOUT_SEC = "Time (seconds) allowed for calls to Elasticsearch";
  public static final String CONFIG_DISPLAY_ES_JETTY_OPERATION_TIMEOUT_SEC = "Time (seconds) allowed for calls HTTP connection to Elasticsearch";

  public static final String CONFIG_NAME_ES_MAX_COMMIT_FAILURES = "es.max.failures";
  public static final String CONFIG_DOCUMENTATION_ES_MAX_COMMIT_FAILURES = "Maximum number of failed attempts to commit an update to Elasticsearch";
  public static final String CONFIG_DISPLAY_ES_MAX_COMMIT_FAILURES = "Maximum number of failed attempts to commit an update to Elasticsearch";

  private Map<String, String> configProps;

  /**
   * Get the version of this connector.
   *
   * @return the version, formatted as a String
   */
  @Override
  public String version() {
    return Version.VERSION;
  }

  /**
   * Start this Connector. This method will only be called on a clean Connector, i.e. it has
   * either just been instantiated and initialized or {@link #stop()} has been invoked.
   *
   * @param props configuration settings
   */
  @Override
  public void start(Map<String, String> props) {
    log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), classname, props);

    configProps = props;
    for (final Entry<String, String> entry : props.entrySet()) {
      String value;
      if (entry.getKey().toLowerCase().contains("password")) {
        value = "[hidden]";
      }
      else {
        value = entry.getValue();
      }
      log.debug("Connector props entry {} : {}", entry.getKey(), value);
    }

    log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), classname);
  }

  /**
   * Returns the Task implementation for this Connector.
   */
  @Override
  public Class<? extends Task> taskClass() {
    return ESSinkTask.class;
  }

  /**
   * Returns a set of configurations for Tasks based on the current configuration,
   * producing at most <count> configurations.
   *
   * @param maxTasks maximum number of configurations to generate
   * @return configurations for Tasks
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.trace("[{}] Entry {}.taskConfigs, maxTasks={}", Thread.currentThread().getId(), classname, maxTasks);

    List<Map<String, String>> taskConfigs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(configProps);
    }

    log.trace("[{}]  Exit {}.taskConfigs, retval={}", Thread.currentThread().getId(), classname, taskConfigs);
    return taskConfigs;
  }

  /**
   * Stop this connector.
   */
  @Override
  public void stop() {
    log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), classname);
    log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), classname);
  }

  /**
   * Define the configuration for the connector.
   * @return The ConfigDef for this connector.
   */
  @Override
  public ConfigDef config() {
    ConfigDef config = new ConfigDef();

    // Connection information
    config.define(CONFIG_NAME_ES_CONNECTION, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, CONFIG_DOCUMENTATION_ES_CONNECTION, CONFIG_GROUP_ES, 1, Width.LONG,
        CONFIG_DISPLAY_ES_CONNECTION);

    config.define(CONFIG_NAME_ES_USER_NAME, Type.STRING, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_USER_NAME, CONFIG_GROUP_ES, 2, Width.MEDIUM,
        CONFIG_DISPLAY_ES_USER_NAME);

    config.define(CONFIG_NAME_ES_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_PASSWORD, CONFIG_GROUP_ES, 3, Width.MEDIUM,
        CONFIG_DISPLAY_ES_PASSWORD);

    config.define(CONFIG_NAME_ES_TLS_KEYSTORE_LOCATION, Type.STRING, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_TLS_KEYSTORE_LOCATION, CONFIG_GROUP_ES,
        10, Width.MEDIUM, CONFIG_DISPLAY_ES_TLS_KEYSTORE_LOCATION);

    config.define(CONFIG_NAME_ES_TLS_KEYSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_TLS_KEYSTORE_PASSWORD, CONFIG_GROUP_ES,
        11, Width.MEDIUM, CONFIG_DISPLAY_ES_TLS_KEYSTORE_PASSWORD);

    config.define(CONFIG_NAME_ES_TLS_TRUSTSTORE_LOCATION, Type.STRING, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_TLS_TRUSTSTORE_LOCATION,
        CONFIG_GROUP_ES, 12, Width.MEDIUM, CONFIG_DISPLAY_ES_TLS_TRUSTSTORE_LOCATION);

    config.define(CONFIG_NAME_ES_TLS_TRUSTSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_TLS_TRUSTSTORE_PASSWORD,
        CONFIG_GROUP_ES, 13, Width.MEDIUM, CONFIG_DISPLAY_ES_TLS_TRUSTSTORE_PASSWORD);

    config.define(CONFIG_NAME_ES_HTTP_PROXY_HOST, Type.STRING, null, Importance.LOW, CONFIG_DOCUMENTATION_ES_HTTP_PROXY_HOST,
        CONFIG_GROUP_ES, 15, Width.MEDIUM, CONFIG_DISPLAY_ES_HTTP_PROXY_HOST);

    config.define(CONFIG_NAME_ES_HTTP_PROXY_PORT, Type.INT, DEFAULT_HTTP_PROXY_PORT, Range.between(0, 65535), Importance.LOW, CONFIG_DOCUMENTATION_ES_HTTP_PROXY_PORT,
        CONFIG_GROUP_ES, 16, Width.MEDIUM, CONFIG_DISPLAY_ES_HTTP_PROXY_PORT);

    // Message reformatting into Elasticsearch documents
    config.define(CONFIG_NAME_ES_DOCUMENT_BUILDER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_DOCUMENT_BUILDER,
        CONFIG_GROUP_ES, 20, Width.MEDIUM, CONFIG_DISPLAY_ES_DOCUMENT_BUILDER);

    // Jetty configuration parameters
    config.define(CONFIG_NAME_ES_JETTY_OPERATION_TIMEOUT_SEC, Type.INT, DEFAULT_JETTY_OPERATION_TIMEOUT_SEC, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_JETTY_OPERATION_TIMEOUT_SEC,
        CONFIG_GROUP_ES, 30, Width.MEDIUM, CONFIG_DISPLAY_ES_JETTY_OPERATION_TIMEOUT_SEC);

    config.define(CONFIG_NAME_ES_JETTY_CONNECTIONS, Type.INT, DEFAULT_JETTY_MAX_CONNECTIONS, Importance.LOW, CONFIG_DOCUMENTATION_ES_JETTY_CONNECTIONS,
        CONFIG_GROUP_ES, 33, Width.MEDIUM, CONFIG_DISPLAY_ES_JETTY_CONNECTIONS);

    config.define(CONFIG_NAME_ES_JETTY_IDLE_TIMEOUT_SEC, Type.INT, DEFAULT_JETTY_IDLE_TIMEOUT_SEC, Importance.LOW, CONFIG_DOCUMENTATION_ES_JETTY_IDLE_TIMEOUT_SEC,
        CONFIG_GROUP_ES, 32, Width.MEDIUM, CONFIG_DISPLAY_ES_JETTY_IDLE_TIMEOUT_SEC);

    config.define(CONFIG_NAME_ES_JETTY_CONNECTION_TIMEOUT_SEC, Type.INT, DEFAULT_JETTY_CONNECTION_TIMEOUT_SEC, Importance.LOW, CONFIG_DOCUMENTATION_ES_JETTY_CONNECTION_TIMEOUT_SEC,
        CONFIG_GROUP_ES, 31, Width.MEDIUM, CONFIG_DISPLAY_ES_JETTY_CONNECTION_TIMEOUT_SEC);

    // Other configuration parameters
    config.define(CONFIG_NAME_ES_MAX_COMMIT_FAILURES, Type.INT, DEFAULT_MAX_COMMIT_FAILURES, Importance.MEDIUM, CONFIG_DOCUMENTATION_ES_MAX_COMMIT_FAILURES,
        CONFIG_GROUP_ES, 35, Width.MEDIUM, CONFIG_DISPLAY_ES_MAX_COMMIT_FAILURES);

    return config;
  }
}
