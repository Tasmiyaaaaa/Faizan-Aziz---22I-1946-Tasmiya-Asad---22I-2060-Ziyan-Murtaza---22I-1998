#!/bin/bash

# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka Server
kafka-server-start.sh config/server.properties &

# Wait for Kafka to start
sleep 10

# Create a Kafka topic
kafka-topics.sh --create --topic topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# Start Kafka Producer
python producer.py &

# Start Kafka Consumers
python consumer_APR.py &
python consumer_AVG.py &
python consumer_PCY.py &

