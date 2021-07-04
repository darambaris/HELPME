#!/bin/bash
# Getting started with Kafka: https://kafka.apache.org/quickstart

# Steps:

## 1. Download file
tar -xzf file_kafka_version.tgz
cd kafka_version

## 2. Start the kafka environment [following commands in order]

# Start the Zookeeper Service
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties 

# Start the Kafka broker service
bin/kafka-server-start.sh -daemon config/server.properties

### Basic Commands 

## Create a topic 
# kafka-topics --create --topic <topic_name> -- partitions <#partitions> --replication-factor <#replicas> --bootstrap-server <host>:<port> 
bin/kafka-topics.sh --create --topic my_first_topic  --partitions 1 --replication-factor 1 --zookeeper localhost:2181

## Produce messages to the topic
# kafka-console-producer --broker-list  <host>:<port> --topic <topic_name>
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my_first_topic

## Consume messages from the topic
# kafka-console-consumer --topic <topic_name> --bootstrap-server <host>:<port> [--from-beginning]
bin/kafka-console-consumer.sh --topic my_first_topic --bootstrap-server localhost:9092 --from-beginning

## Alter the number of partitions to the topic
# kafka-topics --alter --topic <topic_name> --partitions <#new_partitions> --bootstrap-server <host>:<server>
bin/kafka-topics.sh --alter --topic my_first_topic --partitions 3 --zookeeper localhost:2181

# log files directory
# contain the messages in binary format.
ls -alh /var/lib/kafka/data/my_first_topic_0/*.log 

# for more than one partition
ls -alh /var/lib/kafka/data/my_first_topic_*/*.log


# default ports
# Zookeeper - 2181
# Kafka Server - 9092