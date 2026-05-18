# Changelog

## Version 2.0.0 (2026-05-17)

### Major Updates
- **Elasticsearch 9.x Compatibility**: Updated connector to be compatible with Elasticsearch 9.4.9
- **Kafka 4.2.0 Support**: Updated to latest Kafka version 4.2.0
- **Dependency Updates**: Updated all dependencies to latest stable versions
- **TLS 1.3 Support**: Added TLS 1.3 protocol support alongside TLS 1.2 for enhanced security

### Dependency Changes
- **Kafka**: Updated from 3.6.1 to 4.2.0 (latest stable release)
- **Jetty HTTP Client**: Updated to 9.4.57.v20241219 (latest stable 9.4.x release)
- **JUnit Jupiter**: Updated from 5.10.2 to 5.11.4
- **SLF4J**: Updated from 1.7.36 to 2.0.16
- **JSON Library**: Kept at 20240303 (stable release)

### Code Changes
- Updated `ElasticWriter.java` to support both TLS 1.2 and TLS 1.3 protocols
- Enhanced SSL/TLS configuration for better compatibility with Elasticsearch 9.x
- Improved logging for IBM JRE cipher suite exclusions

### Build Configuration
- Updated Java target from 17 to 21
- Updated Maven Surefire plugin from 3.2.5 to 3.5.2
- Updated connector version from 1.0.3 to 2.0.0
- **Added Maven Shade Plugin**: Relocates Jetty classes to avoid conflicts with Kafka 4.2.0's bundled Jetty 12
- Jetty dependencies are now shaded under `com.ibm.eventstreams.shaded.jetty` package

### Testing
- Connector tested and verified compatible with Elasticsearch 9.4.9
- Maintains backward compatibility with Elasticsearch 7.x and 8.x

### Breaking Changes
- Requires Java 21 or later (previously Java 17)
- Requires Kafka 4.2.0 or later (previously 3.6.1)

### Notes
- The connector uses Jetty HTTP client for REST API calls to Elasticsearch
- All Elasticsearch REST API endpoints remain compatible across versions 7.x through 9.x
- TLS 1.3 is now the preferred protocol when available, with TLS 1.2 as fallback

## Version 1.0.3 (Previous)
- Initial stable release
- Support for Elasticsearch 7.0.0 and later
- Basic authentication and TLS support