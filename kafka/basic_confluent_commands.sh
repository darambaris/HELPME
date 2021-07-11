#!/bin/bash

# Install Confluent Platform using only Confluent Community components
# https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html
curl -O http://packages.confluent.io/archive/6.2/confluent-6.2.0.zip

unzip confluent-6.2.0.zip
cd confluent-6.2.0.zip/

# Start Zookeeper Server from Confluent Platform
# http://localhost:2181
bin/zookeeper-server-start -daemon etc/kafka/zookeeper.properties

# Start Kafka Server from Confluent Platform
# http://localhost:9092
bin/kafka-server-start -daemon etc/kafka/server.properties

# Start Schema Registry from Confluent Platform
# http://localhost:8081
bin/schema-registry-start -daemon etc/schema-registry/schema-registry.properties
