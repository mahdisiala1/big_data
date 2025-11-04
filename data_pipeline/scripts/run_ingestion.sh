#!/usr/bin/env bash
set -e
nohup python3 hdfs_sink_crop.py > /tmp/hdfs_sink_crop.log 2>&1 &
nohup python3 hdfs_sink_sensor.py > /tmp/hdfs_sink_sensor.log 2>&1 &
echo "Sinks running"

