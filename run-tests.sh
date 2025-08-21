#!/bin/bash

# Change to the script directory
cd "$(dirname "$0")"

# Run the test script
./docker-test-environment/scripts/test-connector.sh
