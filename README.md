# Kafka Connect sink connector for Elasticsearch

kafka-connect-elastic-sink is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) sink connector for copying data from Apache Kafka into Elasticsearch.

The connector is supplied as source code which you can easily build into a JAR file.

## Contents

- [Building the connector](#building-the-connector)
- [Build Testing and Quickstart](#build-testing-and-quickstart)
- [Running the connector](#running-the-connector)
- [Configuration](#configuration)
- [Document formats](#document-formats)
- [Issues and contributions](#issues-and-contributions)
- [License](#license)

## Building the connector

To build the connector, you must have the following installed:

- [git](https://git-scm.com/)
- [Maven 3.0 or later](https://maven.apache.org)
- Java 8 or later

 Clone the repository with the following command:

 ```shell
 git clone https://github.com/ibm-messaging/kafka-connect-elastic-sink.git
 ```

 Change directory into the `kafka-connect-elastic-sink` directory:

 ```shell
 cd kafka-connect-elastic-sink
 ```

 Build the connector using Maven:

 ```shell
 mvn clean package
 ```

Once built, the output is a single JAR `target/kafka-connect-elastic-sink-<version>-jar-with-dependencies.jar` which contains all of the required dependencies.

## Build Testing and Quickstart

The `quickstart` directory in the repository contains files to run a complete environment as Docker containers. This allows validation of the packages and demonstrates basic usage of the connector.

For more information see the [README](quickstart/README-QS.md) in that directory.

## Running the connector

To run the connector, you must have:

- The JAR from building the connector
- A properties file containing the configuration for the connector
- Apache Kafka 2.0.0 or later, either standalone or included as part of an offering such as IBM Event Streams
- Elasticsearch 7.0.0 or later

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode. It's a good idea to start in standalone mode.

### Running in standalone mode

You need two configuration files. One is for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and the other provides the configuration specific to the Elasticsearch sink connector such as connection information to the server. For the former, the Kafka distribution includes a file called `connect-standalone.properties` that you can use as a starting point. For the latter, you can use `config/elastic-sink.properties` in this repository.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

``` shell
export REPO=<path to cloned repository>
export CLASSPATH=$CLASSPATH:$REPO/target/kafka-connect-elastic-sink-<version>-jar-with-dependencies.jar
bin/connect-standalone.sh config/connect-standalone.properties $(REPO)/config/elastic-sink.properties
```

### Running in distributed mode

You need an instance of Kafka Connect running in distributed mode. The Kafka distribution includes a file called `connect-distributed.properties` that you can use as a starting point.

To start the connector, you can use `config/elastic-sink.json` in this repository
after replacing all placeholders and use a command like this:

``` shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/elastic-sink.json"
```

### Deploying to OpenShift using Strimzi

This repository includes a Kubernetes yaml file called `strimzi.kafkaconnector.yaml` for use with the [Strimzi](https://strimzi.io) operator. Strimzi provides a simplified way of running the Kafka Connect distributed worker, by defining either a KafkaConnect resource or a KafkaConnectS2I resource.

The KafkaConnectS2I resource provides a nice way to have OpenShift do all the work of building the Docker images for you. This works particularly nicely combined with the KafkaConnector resource that represents an individual connector.

The following instructions assume you are running on OpenShift and have Strimzi 0.16 or later installed.

#### Start a Kafka Connect cluster using KafkaConnectS2I

1. Create a file called `kafka-connect-s2i.yaml` containing the definition of a KafkaConnectS2I resource. You can use the examples in the Strimzi project to get started.
1. Configure it with the information it needs to connect to your Kafka cluster. You must include the annotation `strimzi.io/use-connector-resources: "true"` to configure it to use KafkaConnector resources so you can avoid needing to call the Kafka Connect REST API directly.
1. `oc apply -f kafka-connect-s2i.yaml` to create the cluster, which usually takes several minutes.

#### Add the Elasticsearch sink connector to the cluster

1. `mvn clean package` to build the connector JAR.
1. `mkdir my-plugins`
1. `cp target/kafka-connect-elastic-sink-*-jar-with-dependencies.jar my-plugins`
1. `oc start-build <kafkaconnectClusterName>-connect --from-dir ./my-plugins` to add the Elasticsearch sink connector to the Kafka Connect distributed worker cluster. Wait for the build to complete, which usually takes a few minutes.
1. `oc describe kafkaconnects2i <kafkaConnectClusterName>` to check that the Elasticsearch sink connector is in the list of available connector plugins.

#### Start an instance of the Elasticsearch sink connector using KafkaConnector

1. `cp deploy/strimzi.kafkaconnector.yaml kafkaconnector.yaml`
1. Update the `kafkaconnector.yaml` file to replace all of the values in `<>`, adding any additional configuration properties.
1. `oc apply -f kafkaconnector.yaml` to start the connector.
1. `oc get kafkaconnector` to list the connectors. You can use `oc describe` to get more details on the connector, such as its status.

## Configuration

See this [configuration file](config/elastic-sink.properties) for all exposed configuration attributes. Required attributes for the connector are
the address of the Elasticsearch server, and the Kafka topics that will be collected. You do not include the protocol (http or https) as part
of the server address.

The only other required properties are the `es.document.builder` and `es.index.builder` classes which should not be changed unless you write your
own Java classes for alternative document and index formatting.

Additional properties allow for security configuration and tuning of how the calls are made to the Elasticsearch server.

The configuration options for the Kafka Connect sink connector for Elasticsearch are as follows:

| Name                       | Description                                                            | Type    | Default        | Valid values                         |
|----------------------------|------------------------------------------------------------------------|---------|----------------|--------------------------------------|
| topics                     | A list of topics to use as input                                       | string  |                | topic1[,topic2,...]                  |
| es.connection              | The connection endpoint for Elasticsearch                              | string  |                | host:port                            |
| es.document.builder        | The class used to build the document content                           | string  |                | Class implementing DocumentBuilder   |
| es.identifier.builder      | The class used to build the document identifier                        | string  |                | Class implementing IdentifierBuilder |
| es.index.builder           | The class used to build the document index                             | string  |                | Class implementing IndexBuilder      |
| key.converter              | The class used to convert Kafka record keys                            | string  |                | See below                            |
| value.converter            | The class used to convert Kafka record values                          | string  |                | See below                            |
| es.http.connections        | The maximum number of HTTP connections to Elasticsearch                | integer | 5              |                                      |
| es.http.timeout.idle       | Timeout (seconds) for idle HTTP connections to Elasticsearch           | integer | 30             |                                      |
| es.http.timeout.connection | Time (seconds) allowed for initial HTTP connection to Elasticsearch    | integer | 10             |                                      |
| es.http.timeout.operation  | Time (seconds) allowed for calls to Elasticsearch                      | integer | 6              |                                      |
| es.max.failures            | Maximum number of failed attempts to commit an update to Elasticsearch | integer | 5              |                                      |
| es.http.proxy.host         | Hostname for HTTP proxy                                                | string  |                | Hostname                             |
| es.http.proxy.port         | Port number for HTTP proxy                                             | integer | 8080           | Port number                          |
| es.user.name               | The user name for authenticating with Elasticsearch                    | string  |                |                                      |
| es.password                | The password for authenticating with Elasticsearch                     | string  |                |                                      |
| es.tls.keystore.location   | The path to the keystore to use for TLS connections                    | string  | JVM keystore   | Local path to a keystore file        |
| es.tls.keystore.password   | The password of the keystore to use for TLS connections                | string  |                |                                      |
| es.tls.keystore.type       | The type of the keystore to use for TLS connections                    | string  | JKS            | JKS, PKCS12, etc.                    |
| es.tls.truststore.location | The path to the truststore to use for TLS connections                  | string  | JVM truststore | Local path to a truststore file      |
| es.tls.truststore.password | The password of the truststore to use for TLS connections              | string  |                |                                      |
| es.tls.truststore.type     | The type of the truststore to use for TLS connections                  | string  | JKS            | JKS, PKCS12, etc.                    |
| es.tls.skip.verification   | Whether to skip SSL verification for the Elasticsearch connection      | boolean | false          | true, false                          |

### Supported configurations

In order to ensure reliable indexing of documents, the following configurations are only supported with these values:

| Name            | Valid values                                                                                      |
|-----------------|---------------------------------------------------------------------------------------------------|
| key.converter   | `org.apache.kafka.connect.storage.StringConverter`, `org.apache.kafka.connect.json.JsonConverter` |
| value.converter | `org.apache.kafka.connect.json.JsonConverter`                                                     |

### Performance tuning

Multiple instances of the connector can be run in parallel by setting the `tasks.max` configuration property. This should usually be set to match the number of partitions defined for the Kafka topic.

### Security

The connector supports anonymous and basic authentication to the Elasticsearch server. With basic authentication, you need to provide a userid and password
as part of the configuration.

For TLS-protected communication to Elasticsearch, you must provide appropriate certificate configuration. At minimum, a truststore is needed. Your Elasticsearch server configuration will determine whether individual certificates (a keystore populated with your personal certificate) are also needed.

### Externalizing passwords with FileConfigProvider

Given a file `es-secrets.properties` with the contents:

```properties
secret-key=password
```

Update the worker configuration file to specify the FileConfigProvider which is included by default:

```properties
# Additional properties for the worker configuration to enable use of ConfigProviders
# multiple comma-separated provider types can be specified here
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Update the connector configuration file to reference `secret-key` in the file:

```properties
es.password=${file:es-secret.properties:secret-key}
```

## Document formats

The documents inserted into Elasticsearch by this connector are JSON objects. The conversion from the incoming Kafka records to the Elasticsearch documents is performed using a *document builder*. The supplied `JsonDocumentBuilder` converts the Kafka record's value into a document containing a JSON object. The connector inserts documents into the store using Elasticsearch type `_doc`.

To ensure the documents can be indexed reliably, the incoming Kafka records must also be JSON objects. This is ensured by setting the `value.converter` configuration to `org.apache.kafka.connect.json.JsonConverter` which only accepts well-formed JSON objects.

The name of the Elasticsearch index is same as the Kafka topic name, converted into lower case with special characters replaced.

## Modes of operation

There are two modes of operation based on whether you want each Kafka record to create a new document in Elasticsearch, or whether you want Kafka records with the same key to replace earlier versions of documents in Elasticsearch.

### Unique document ID

By setting the `es.identifier.builder` configuration to `com.ibm.eventstreams.connect.elasticsink.builders.DefaultIdentifierBuilder`, the document ID is a concatenation of the topic name, partition and record offset, for example `topic1!0!42`. This means that each Kafka record creates a unique document ID and will result in a separate document in Elasticsearch. The records do not need to have keys.

This mode of operation is suitable if the Kafka records are independent events and you want each of them to be indexed in Elasticsearch separately.

### Document ID based on Kafka record key

By setting the `es.identifier.builder` configuration to `com.ibm.eventstreams.connect.elasticsink.builders.KeyIdentifierBuilder`, each Kafka record replaces any existing document in Elasticsearch which has the same key. The Kafka record key is used as the document ID. This means the document IDs are only as unique as the Kafka record keys. The records must have keys.

This mode of operation is suitable if the Kafka records represent data items identified by the record key, and where a sequence of records with the same key represent updates to a particular data item. In this case, you want the most recent version of the data item to be indexed. A record with an empty value is treated as deletion of the data item and results in deletion of a document with that key from the index.

This mode of operation is suitable if you are using the change data capture technique.

## Support

Commercial support for this connector is available for customers with a support entitlement for [IBM Event Automation](https://www.ibm.com/products/event-automation) or [IBM Cloud Pak for Integration](https://www.ibm.com/cloud/cloud-pak-for-integration).

## Testing the Connector

A test environment has been set up to validate the connector functionality, including the new SSL verification skip and keystore/truststore type options. The test infrastructure is organized in the `docker-test-environment` directory.

### Test Infrastructure Organization

```shell
docker-test-environment/
├── config/                  # Configuration files for testing
│   ├── connect-distributed.properties
│   └── elastic-sink-test.json
├── docker-compose.yml       # Docker Compose configuration
├── Dockerfile               # Dockerfile for the Kafka Connect container
└── scripts/                 # Test scripts
    └── test-connector.sh    # Main test script
```

### Prerequisites

- Docker and Docker Compose
- Maven
- jq (for JSON processing)

### Running the Tests

   ```shell
   ./run-tests.sh
   ```

This script will:

- Build the connector
- Start a test environment with Kafka, Kafka Connect, and Elasticsearch
- Create a test topic
- Deploy the connector with the new configuration options
- Send a test message
- Verify the message was indexed in Elasticsearch

### Manual Testing

You can also manually test the connector:

1. Navigate to the test environment directory:

   ```shell
   cd docker-test-environment
   ```

2. Build the Docker images and start the services:

   ```shell
   docker-compose up -d
   ```

3. Deploy the connector:

   ```shell
   curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors -d @config/elastic-sink-test.json
   ```

4. Check the connector status:

   ```shell
   curl -s http://localhost:8083/connectors/elastic-sink-test/status
   ```

5. Send test messages to the Kafka topic and verify they appear in Elasticsearch.

## Issues and contributions

For issues relating specifically to this connector, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-elastic-sink/issues). If you do want to submit a Pull Request related to this connector, please read the [contributing guide](CONTRIBUTING.md) first to understand how to sign your commits.

## License

Copyright 2020, 2023 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.
