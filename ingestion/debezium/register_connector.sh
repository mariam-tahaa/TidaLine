#!/bin/bash

CONNECTOR_NAME="tidaline-connector"

curl -X DELETE \
  http://localhost:8083/connectors/$CONNECTOR_NAME

curl -X POST \
  http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @ingestion/debezium/connector.json

curl http://localhost:8083/connectors