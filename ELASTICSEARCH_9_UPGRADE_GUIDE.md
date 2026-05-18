# Elasticsearch 9.x Upgrade Guide

## Overview
This guide helps you upgrade the Kafka Connect Elasticsearch Sink Connector to version 2.0.0, which adds support for Elasticsearch 9.4.9 and later versions.

## What's New in Version 2.0.0

### Elasticsearch Compatibility
- **Full support for Elasticsearch 9.4.9**
- Maintains backward compatibility with Elasticsearch 7.x and 8.x
- Enhanced TLS/SSL support with TLS 1.3

### Key Changes
1. **Updated Dependencies**: All dependencies updated to latest stable versions
2. **Java 21 Required**: Minimum Java version increased from 17 to 21
3. **Kafka 4.2.0**: Updated to latest Kafka client libraries (4.2.0)
4. **TLS 1.3 Support**: Added alongside TLS 1.2 for better security

## Prerequisites

Before upgrading, ensure you have:
- Java 21 or later installed
- Maven 3.0 or later
- Kafka 4.2.0 or later
- Elasticsearch 7.0.0 or later (tested with 9.4.9)

## Upgrade Steps

### 1. Backup Current Configuration
Save your current connector configuration files:
```bash
cp config/elastic-sink.properties config/elastic-sink.properties.backup
cp config/elastic-sink.json config/elastic-sink.json.backup
```

### 2. Build the New Connector
```bash
cd kafka-connect-elastic-sink
mvn clean package
```

This will create: `target/kafka-connect-elastic-sink-2.0.0-jar-with-dependencies.jar`

### 3. Stop Existing Connector
If running in standalone mode:
```bash
# Stop the Kafka Connect process
```

If running in distributed mode:
```bash
# Delete the connector
curl -X DELETE http://localhost:8083/connectors/your-connector-name
```

### 4. Replace JAR File
Replace the old JAR with the new one in your Kafka Connect plugins directory.

### 5. Update Configuration (if needed)
Review your configuration files. Most settings remain the same, but verify:
- TLS/SSL settings are correct
- Connection strings are valid
- Authentication credentials are current

### 6. Start the Connector
Standalone mode:
```bash
export CLASSPATH=$CLASSPATH:/path/to/kafka-connect-elastic-sink-2.0.0-jar-with-dependencies.jar
bin/connect-standalone.sh config/connect-standalone.properties config/elastic-sink.properties
```

Distributed mode:
```bash
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/elastic-sink.json"
```

## Configuration Changes

### No Breaking Configuration Changes
All existing configuration parameters remain the same. The connector will automatically:
- Use TLS 1.3 when available (falls back to TLS 1.2)
- Work with Elasticsearch 9.x REST API endpoints
- Maintain compatibility with older Elasticsearch versions

### Recommended Settings for Elasticsearch 9.x

For optimal performance with Elasticsearch 9.x:

```properties
# Connection settings
es.connection=your-es-host:9200
es.user.name=your-username
es.password=your-password

# TLS settings (if using HTTPS)
es.tls.truststore.location=/path/to/truststore.jks
es.tls.truststore.password=truststore-password

# Performance tuning
es.http.connections=10
es.http.timeout.operation=10
es.max.failures=5

# Document builders (no change needed)
es.document.builder=com.ibm.eventstreams.connect.elasticsink.builders.JsonDocumentBuilder
es.index.builder=com.ibm.eventstreams.connect.elasticsink.builders.DefaultIndexBuilder
es.identifier.builder=com.ibm.eventstreams.connect.elasticsink.builders.DefaultIdentifierBuilder
```

## Testing the Upgrade

### 1. Verify Connectivity
Check that the connector can connect to Elasticsearch 9.x:
```bash
# Check connector status
curl http://localhost:8083/connectors/your-connector-name/status
```

### 2. Test Data Flow
Send test messages to your Kafka topic and verify they appear in Elasticsearch:
```bash
# Check Elasticsearch index
curl -X GET "https://your-es-host:9200/your-index/_search?pretty"
```

### 3. Monitor Logs
Watch for any errors or warnings:
```bash
tail -f logs/connect.log
```

## Troubleshooting

### Connection Issues
If you experience connection problems:
1. Verify Elasticsearch 9.x is running and accessible
2. Check TLS/SSL certificates are valid
3. Ensure firewall rules allow connections
4. Verify authentication credentials

### TLS/SSL Issues
If you see SSL/TLS errors:
1. Ensure your truststore contains the Elasticsearch CA certificate
2. Verify TLS 1.2 or 1.3 is enabled on Elasticsearch
3. Check certificate validity dates

### Performance Issues
If throughput is lower than expected:
1. Increase `es.http.connections` (default: 5, try 10-20)
2. Adjust `es.http.timeout.operation` (default: 6 seconds)
3. Increase `tasks.max` to match topic partitions

## Rollback Procedure

If you need to rollback to version 1.0.3:

1. Stop the connector
2. Replace the JAR with version 1.0.3
3. Restore backup configuration files
4. Restart the connector

## Elasticsearch 9.x Specific Notes

### API Compatibility
- All REST API endpoints used by the connector are compatible with ES 9.x
- The `_bulk` API endpoint works identically across versions
- Document indexing uses the `_doc` type (compatible with all versions)

### Security
- TLS 1.3 is preferred when available
- Basic authentication continues to work
- Certificate-based authentication is supported

### Performance
- Elasticsearch 9.x may have different performance characteristics
- Monitor and adjust timeout settings as needed
- Consider increasing connection pool size for high-throughput scenarios

## Support

For issues specific to this connector:
- GitHub Issues: https://github.com/ibm-messaging/kafka-connect-elastic-sink/issues
- IBM Event Automation Support (for commercial customers)

For Elasticsearch 9.x issues:
- Elasticsearch Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/9.4/index.html
- Elastic Support (for commercial customers)

## Additional Resources

- [Connector README](README.md)
- [Configuration Reference](config/elastic-sink.properties)
- [Changelog](CHANGELOG.md)
- [Elasticsearch 9.x Release Notes](https://www.elastic.co/guide/en/elasticsearch/reference/9.4/release-notes.html)