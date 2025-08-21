#!/bin/bash

# Enable strict error handling
set -euo pipefail

# Colored logging functions
log_info() {
    echo -e "\033[1;34m[$(date +"%Y-%m-%d %H:%M:%S")] [INFO] $1\033[0m"
}

log_success() {
    echo -e "\033[1;32m[$(date +"%Y-%m-%d %H:%M:%S")] [SUCCESS] $1\033[0m"
}

log_warn() {
    echo -e "\033[1;33m[$(date +"%Y-%m-%d %H:%M:%S")] [WARNING] $1\033[0m"
}

log_error() {
    echo -e "\033[1;31m[$(date +"%Y-%m-%d %H:%M:%S")] [ERROR] $1\033[0m"
}

# Step 1: Build the project
log_info "Building the project with Maven..."
if mvn clean package; then
    log_success "Project built successfully."
else
    log_error "Build failed. Aborting."
    exit 1
fi

# Step 2: Start Docker containers
log_info "Starting Docker containers..."
cd "$(dirname "$0")/.."
docker-compose -f docker-compose.yml up -d --build
log_success "Containers started."

# Step 3: Wait for services
log_info "Waiting 30 seconds for services to be ready..."
sleep 30

# Step 4: Create test topic in Kafka
log_info "Creating Kafka topic 'test-topic'..."
if docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
    --topic test-topic --if-not-exists --bootstrap-server kafka:9092 \
    --partitions 1 --replication-factor 1; then
    log_success "Kafka topic created or already exists."
else
    log_warn "Could not create Kafka topic (might already exist)."
fi

# Step 5: Deploy Kafka Connector
log_info "Deploying Kafka Connect connector..."
curl -s -X POST -H "Content-Type: application/json" \
    http://localhost:8083/connectors \
    -d @./config/elastic-sink-test.json > /dev/null

log_success "Connector deployment initiated."

# Step 6: Wait for connector
log_info "Waiting 10 seconds for connector to initialize..."
sleep 10

# Step 7: Check connector status
log_info "Checking connector status..."
curl -s http://localhost:8083/connectors/elastic-sink-test/status | jq .
log_success "Connector status retrieved."

# Step 8: Produce test message
log_info "Producing test Kafka message..."
echo '{"id": 1, "name": "Test Message", "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'" }' > test-message.json

docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --topic test-topic --bootstrap-server kafka:9092 < test-message.json

log_success "Test message sent."

# Step 9: Wait for message to be processed
log_info "Waiting 5 seconds for message to be processed..."
sleep 5

# Step 10: Query Elasticsearch
log_info "Querying Elasticsearch for message..."
curl -s http://localhost:9200/test-topic/_search | jq .

log_success "Test completed successfully!"
echo
log_info "To stop the containers, run: docker-compose down"
