# Kafka Connect sink connector for Elasticsearch
kafka-connect-es-sink is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect)
sink connector for copying data from Apache Kafka into Elasticsearch.

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

Once built, the output is a single JAR `target/kafka-connect-es-sink-<version>-jar-with-dependencies.jar` which
contains all of the required dependencies.

## Build Testing and Quickstart
The `quickstart` directory in the repository contains files to run a complete
environment as Docker containers. This allows validation of the packages and demonstrates
basic usage of the connector.

For more information see the [README](quickstart/README-QS.md) in that directory.

## Running the connector
To run the connector, you must have:
* The JAR from building the connector
* A properties file containing the configuration for the connector
* Apache Kafka 2.0.0 or later, either standalone or included as part of an offering such as IBM Event Streams

The connector can be run in a Kafka Connect worker in either standalone
(single process) or distributed mode. It's a good idea to start in standalone mode.

### Running in standalone mode
You need two configuration files. One is for the configuration that applies to all
of the connectors such as the Kafka bootstrap servers, and the other provides the
configuration specific to the Elasticsearch sink connector such as connection
information to the server. For the former, the Kafka distribution includes a
file called `connect-standalone.properties` that you can use as a starting point.
For the latter, you can use `config/es-sink.properties` in this repository.

To run the connector in standalone mode from the directory into which you
installed Apache Kafka, you use a command like this:

``` shell
$ export REPO=<path to cloned repository>
$ export CLASSPATH=$CLASSPATH:$REPO/target/kafka-connect-es-sink-0.0.1-SNAPSHOT-jar-with-dependencies.jar
$ ./bin/connect-standalone.sh config/connect-standalone.properties $(REPO)/config/es-sink.properties
```

### Running in distributed mode
You need an instance of Kafka Connect running in distributed mode. The Kafka distribution includes
a file called `connect-distributed.properties` that you can use as a starting point.

To start the connector, you can use `config/es-sink.json` in this repository
after replacing all placeholders and use a command like this:

``` shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/es-sink.json"
```

## Configuration
See this [configuration file](config/es-sink.properties) for all exposed
configuration attributes. Required attributes for the connector are
the address of the Elasticsearch server, and the Kafka topics that will
be collected. You do not include the protocol (http or https) as part
of the server address.

The only other required property is the `es.document.builder` class
which should not be changed unless you write your own Java class for an
alternative document formatter.

Additional properties allow for security configuration and tuning of how the
calls are made to the Elasticsearch server.

### Performance tuning
Multiple instances of the connector can be run in parallel by setting the
`tasks.max` configuration property. This should usually be set to match
the number of partitions defined for the Kafka topic.

### Security
The connector supports anonymous and basic authentication to the Elasticsearch
server. With basic authentication, you need to provide a userid and password
as part of the configuration.

For TLS-protected communication to Elasticsearch, you must provide
appropriate certificate configuration. At minimum, a truststore is needed.
Your Elasticsearch server configuration will determine whether individual
certificates (a keystore populated with your personal certificate) are also
needed.

### Externalizing passwords with FileConfigProvider
Given a file `es-secrets.properties` with the contents:

```
secret-key=password
```

Update the worker configuration file to specify the FileConfigProvider which is included by default:

```
# Additional properties for the worker configuration to enable use of ConfigProviders
# multiple comma-separated provider types can be specified here
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Update the connector configuration file to reference `secret-key` in the file:

```
es.password=${file:es-secret.properties:secret-key}
```

## Document formats
The Elasticsearch document created by this connector is a JSON object.
The object is created from the Kafka message as the `body` element, and is
inserted as Elasticsearch type `_doc` to the store.

The Kafka schema and key values, if known, are also inserted as fields in the JSON object.

The `key.converter` and `value.converter` classes are used by the connector
framework to take the message from Kafka and convert it before it gets passed to
this connector. These attributes are configured in the sample properties files
to use String converters.

### Document identification
When inserting the document into Elasticsearch, an *index* and a unique *identifier*
are needed. The identifier is used to ensure that if a document is inserted twice
with the same identifier, only one copy of the document is stored (although a
revision count for the document may be incremented).

In this implementation, the *index* is taken from the Kafka topic (flattened
and lowercased).

The *identifier* is generated from a combination of the topic, Kafka offset,
Kafka partition, and the message creation timestamp. That should make unique
but repeatable identifiers so that if there is a problem and the connector
restarts or goes through a retry process, the same document is recreated.

## Issues and contributions
For issues relating specifically to this connector, please use
the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-elastic-sink/issues).
If you do want to submit a Pull Request related to this connector, please read
the [contributing guide](CONTRIBUTING.md) first to understand how to sign
your commits.

## License
Copyright 2020 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    (http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.
