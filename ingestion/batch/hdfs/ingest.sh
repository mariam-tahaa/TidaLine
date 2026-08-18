#!/bin/bash

set -e

echo "Waiting for HDFS..."

until hdfs dfs -ls / > /dev/null 2>&1; do
    sleep 5
done

echo "HDFS is ready."

hdfs dfs -mkdir -p /bronze/ports

for file in /data/ports/*.csv; do
    filename=$(basename "$file")

    echo "Uploading $filename..."

    hdfs dfs -put -f "$file" "/bronze/ports/$filename"
done

echo "Ingestion completed."