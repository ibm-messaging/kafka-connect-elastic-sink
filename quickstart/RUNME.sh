#!/bin/bash

# Copyright 2020 IBM Corporation
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script builds and creates a simple configuration for the Kafka Connect sink connector for Elasticsearch
#
# Configuration files in this directory are used as part of the process - they are slightly different
# from the configuration files elsewhere in the repository, as they can use hostnames from the docker-compose
# network.
#
# There are some delays coded in this script to try to ensure containers are up and running before doing any
# real activity.
#
# Prereqs: docker-compose
#          mvn (to compile the program)
#          curl
#          jq (JSON script processor)
curdir=`pwd`
rc=0

if [ -z "$DOCKER_EXEC" ]; then
  DOCKER_EXEC="docker"
fi

cd ..

# Compile the code
mvn clean package
if [ $? -ne 0 ]
then
  exit 1
fi

# Build the generated connector jar into a container
${DOCKER_EXEC} build --rm -t kafka-connect-elastic-sink:latest -f quickstart/Dockerfile .
if [ $? -ne 0 ]
then
  exit 1
fi

# Restart everything from empty. The yaml file defining the
# containers has no persistent volumes.
cd $curdir
project="--project-name qs"
docker-compose $project down
docker-compose $project up -d

# Give it a chance to start
echo "Waiting for 60 seconds..."
sleep 60

# Create a topic that we will use for testing
TOPIC=ESSINKTEST

echo "Creating Kafka topic"
docker-compose $project exec kafka \
 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
                  --create --topic $TOPIC \
                  --partitions 1  --replication-factor 1 >/dev/null 2>&1

# Publish a few lines of text as events
echo "Publishing to topic"
docker-compose $project exec -T kafka  \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic $TOPIC << EOF | sed "s/>//g"
{"Event": "0"}
{"Event": "1"}
{"Event": "2"}
{"Event": "3"}
{"Event": "4"}
{"Event": "5"}
{"Event": "6"}
{"Event": "7"}
{"Event": "8"}
{"Event": "9"}
EOF
echo

# Wait a while longer to give everything a chance to stabilise
echo "Waiting for 60 seconds..."
sleep 60

# And we should now see the output count matching the number of lines in this file.
# Note: Needs the 'jq' command to be installed for JSON parsing and  'curl'
c=`curl -k -Ss -H 'Content-Type: application/json' http://localhost:9200/essinktest/_count | jq '.count'`
echo "Transferred documents = $c"
if [ "$c" = "10" ]
then
  echo "OK"
  rc=0
else
  echo "Failed"
  # docker-compose $project logs connector
  rc=1
fi
echo
exit $rc
