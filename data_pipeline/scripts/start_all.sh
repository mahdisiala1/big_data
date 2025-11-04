#!/usr/bin/env bash
set -e
nohup /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties > /tmp/zk.log 2>&1 &
sleep 2
nohup /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties > /tmp/kafka.log 2>&1 &
sleep 3
/usr/local/kafka/bin/kafka-topics.sh --create --topic agriculture.crop.data --zookeeper localhost:2181 --partitions 3 --replication-factor 1 || true
/usr/local/kafka/bin/kafka-topics.sh --create --topic agriculture.sensor.data --zookeeper localhost:2181 --partitions 3 --replication-factor 1 || true
echo "ZK/Kafka/Topics OK"

