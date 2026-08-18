#!/bin/bash

set -e

echo "Starting DataNode..."

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Use in-container tmp directory for data (Windows compatibility)
mkdir -p /tmp/hadoop/dfs/data

echo "DataNode data directory ready at /tmp/hadoop/dfs/data"
exec hdfs datanode