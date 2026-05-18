# Build and Test Instructions for Elasticsearch 9.x Compatibility

## Quick Start

### Build the Connector
```bash
# Navigate to the project directory
cd kafka-connect-elastic-sink

# Clean and build
mvn clean package

# The JAR will be created at:
# target/kafka-connect-elastic-sink-2.0.0-jar-with-dependencies.jar
```

## Prerequisites

### Required Software
- **Java 21 or later** (JDK)
- **Maven 3.0 or later**
- **Git** (for cloning the repository)

### Verify Prerequisites
```bash
# Check Java version (should be 21 or higher)
java -version

# Check Maven version
mvn -version

# Check Git version
git --version
```

## Building from Source

### 1. Clone the Repository (if not already done)
```bash
git clone https://github.com/ibm-messaging/kafka-connect-elastic-sink.git
cd kafka-connect-elastic-sink
```

### 2. Build the Project
```bash
# Full build with tests
mvn clean package

# Build without tests (faster)
mvn clean package -DskipTests

# Build with verbose output
mvn clean package -X
```

### 3. Verify Build Output
```bash
# Check the JAR file was created
ls -lh target/kafka-connect-elastic-sink-2.0.0-jar-with-dependencies.jar

# The file should be approximately 2-3 MB
```

## Testing with Elasticsearch 9.4.9

### Option 1: Docker Compose (Recommended for Testing)

Create a `docker-compose-test.yml` file:

```yaml
version: '3.8'
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:9.4.9
    container_name: es-test
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    networks:
      - kafka-es-network

  kafka:
    image: apache/kafka:4.2.0
    container_name: kafka-test
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    networks:
      - kafka-es-network

networks:
  kafka-es-network:
    driver: bridge
```

Start the test environment:
```bash
docker-compose -f docker-compose-test.yml up -d

# Wait for services to be ready (about 30 seconds)
sleep 30

# Verify Elasticsearch is running
curl http://localhost:9200

# Verify Kafka is running
docker exec kafka-test kafka-topics --list --bootstrap-server localhost:9092
```

### Option 2: Manual Testing with Existing Infrastructure

If you have Elasticsearch 9.4.9 and Kafka already running:

#### 1. Create Test Configuration

Create `config/elastic-sink-test.properties`:
```properties
name=elastic-sink-test
connector.class=com.ibm.eventstreams.connect.elasticsink.ElasticSinkConnector
tasks.max=1
topics=test-topic

# Elasticsearch connection
es.connection=localhost:9200
es.user.name=
es.password=

# Document builders
es.document.builder=com.ibm.eventstreams.connect.elasticsink.builders.JsonDocumentBuilder
es.index.builder=com.ibm.eventstreams.connect.elasticsink.builders.DefaultIndexBuilder
es.identifier.builder=com.ibm.eventstreams.connect.elasticsink.builders.DefaultIdentifierBuilder

# Converters
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

#### 2. Create Test Topic
```bash
# Create a test topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1
```

#### 3. Run Connector in Standalone Mode
```bash
# Set classpath
export CLASSPATH=$CLASSPATH:$(pwd)/target/kafka-connect-elastic-sink-2.0.0-jar-with-dependencies.jar

# Run connector
connect-standalone \
  $KAFKA_HOME/config/connect-standalone.properties \
  config/elastic-sink-test.properties
```

#### 4. Send Test Messages
In a new terminal:
```bash
# Send test JSON messages
echo '{"name":"John Doe","age":30,"city":"New York"}' | \
  kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-topic

echo '{"name":"Jane Smith","age":25,"city":"San Francisco"}' | \
  kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

#### 5. Verify Data in Elasticsearch
```bash
# Check if index was created
curl http://localhost:9200/_cat/indices?v

# Search for documents
curl http://localhost:9200/test-topic/_search?pretty

# Expected output should show the test documents
```

## Automated Testing

### Run Unit Tests
```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=ElasticSinkConnectorTest

# Run tests with coverage
mvn clean test jacoco:report
```

### Integration Testing Script

Create `test-integration.sh`:
```bash
#!/bin/bash
set -e

echo "Starting integration test..."

# Build the connector
echo "Building connector..."
mvn clean package -DskipTests

# Start test environment
echo "Starting Docker containers..."
docker-compose -f docker-compose-test.yml up -d

# Wait for services
echo "Waiting for services to be ready..."
sleep 30

# Verify Elasticsearch
echo "Verifying Elasticsearch..."
curl -f http://localhost:9200 || exit 1

# Create test topic
echo "Creating test topic..."
docker exec kafka-test kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic integration-test \
  --partitions 1 \
  --replication-factor 1 || true

# Run connector (in background)
echo "Starting connector..."
export CLASSPATH=$CLASSPATH:$(pwd)/target/kafka-connect-elastic-sink-2.0.0-jar-with-dependencies.jar
connect-standalone \
  config/connect-standalone.properties \
  config/elastic-sink-test.properties &
CONNECTOR_PID=$!

# Wait for connector to start
sleep 10

# Send test messages
echo "Sending test messages..."
for i in {1..10}; do
  echo "{\"id\":$i,\"message\":\"Test message $i\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" | \
    docker exec -i kafka-test kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic integration-test
done

# Wait for processing
sleep 5

# Verify data in Elasticsearch
echo "Verifying data in Elasticsearch..."
RESULT=$(curl -s http://localhost:9200/integration-test/_search | jq '.hits.total.value')

if [ "$RESULT" -eq 10 ]; then
  echo "✓ Integration test PASSED: All 10 messages indexed successfully"
  EXIT_CODE=0
else
  echo "✗ Integration test FAILED: Expected 10 documents, found $RESULT"
  EXIT_CODE=1
fi

# Cleanup
echo "Cleaning up..."
kill $CONNECTOR_PID 2>/dev/null || true
docker-compose -f docker-compose-test.yml down -v

exit $EXIT_CODE
```

Make it executable and run:
```bash
chmod +x test-integration.sh
./test-integration.sh
```

## Troubleshooting Build Issues

### Java Version Issues
```bash
# If you have multiple Java versions, set JAVA_HOME
export JAVA_HOME=/path/to/java-21
export PATH=$JAVA_HOME/bin:$PATH
```

### Maven Dependency Issues
```bash
# Clear Maven cache and rebuild
rm -rf ~/.m2/repository/com/ibm/eventstreams
mvn clean install -U
```

### Build Failures
```bash
# Build with debug output
mvn clean package -X -e

# Skip tests if they're failing
mvn clean package -DskipTests
```

## Performance Testing

### Load Test Script
Create `load-test.sh`:
```bash
#!/bin/bash

MESSAGES=10000
TOPIC="load-test"

echo "Sending $MESSAGES messages to $TOPIC..."

for i in $(seq 1 $MESSAGES); do
  echo "{\"id\":$i,\"data\":\"Load test message $i\",\"timestamp\":$(date +%s)}" | \
    kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC
  
  if [ $((i % 1000)) -eq 0 ]; then
    echo "Sent $i messages..."
  fi
done

echo "Load test complete. Check Elasticsearch for results."
```

## Continuous Integration

### GitHub Actions Example
Create `.github/workflows/build-test.yml`:
```yaml
name: Build and Test

on: [push, pull_request]

jobs:
  build:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up JDK 21
      uses: actions/setup-java@v3
      with:
        java-version: '21'
        distribution: 'temurin'
    
    - name: Build with Maven
      run: mvn clean package
    
    - name: Run tests
      run: mvn test
    
    - name: Upload JAR
      uses: actions/upload-artifact@v3
      with:
        name: kafka-connect-elastic-sink
        path: target/kafka-connect-elastic-sink-*.jar
```

## Next Steps

After successful build and testing:
1. Review the [Upgrade Guide](ELASTICSEARCH_9_UPGRADE_GUIDE.md)
2. Check the [Changelog](CHANGELOG.md) for all changes
3. Deploy to your production environment following the upgrade guide
4. Monitor logs and performance metrics

## Support

For build or test issues:
- Check the [GitHub Issues](https://github.com/ibm-messaging/kafka-connect-elastic-sink/issues)
- Review the [README](README.md) for general information
- Contact IBM Event Automation Support (for commercial customers)