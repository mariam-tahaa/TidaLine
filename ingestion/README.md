## How to work with CDC & Kafka

### Connector.json

- contains the configuration of the Debezium PostgreSQL source connector.
- It is the main configuration that tells Debezium:

1. Which PostgreSQL database to connect to
2. How to authenticate with PostgreSQL
3. Which PostgreSQL tables to monitor
4. Which logical replication plugin to use
5. What prefix should be used for Kafka topics
6. How CDC records should be serialized
7. Which Schema Registry should be used, if Avro serialization is configured

- it defines the CDC source and how Debezium should publish the captured changes.

---

## register_connector.sh

- A shell script used to register the connector configuration with Kafka Connect.
- it takes the configuration from: `connector.json`

---

## Test

### 1. Check all containers

```bash
docker compose -f docker/case-study-docker-compose.yaml ps
```

*Result Should Be :*

```text
kafka
postgres
kafka-connect
schema-registry
stream_extractor
sftp
```
---

### 2. Verify PostgreSQL

#### 2.1 Connect to PostgreSQL:

```bash
docker exec -it postgres psql -U postgres -d maritime_logistics
```

#### 2.2 Then:

`\dt`

*Result :*

` public | earthquakes `

#### 2.3 Then:

```sql
SELECT * FROM earthquakes;
```
---

### 3. Verify PostgreSQL logical replication

#### 3.1 Inside psql:

```sql
SHOW wal_level;
```

*Result :*

`logical`

- Which means WAL of postgres save logical information or enough info for Debezium to get its events

#### 3.2 Then:

`SELECT slot_name, plugin, active
FROM pg_replication_slots;`

*Result :*

```sql
slot_name                 | plugin  | active
--------------------------+---------+-------
debezium                   | pgoutput| t
```
---

### 4. Verify Kafka Connect

#### 4.1 Check the Kafka Connect API:

```bash
Invoke-RestMethod http://localhost:8083/connectors
```

*Result :*

```bash
[
  "tidaline-connector"
]
```
---

### 5. Check connector status

```bash
Invoke-RestMethod `
  http://localhost:8083/connectors/tidaline-connector/status
```
*Result :*

```json
{
  "name": "test2-connector",
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "state": "RUNNING"
    }
  ]
}
```
---

### 6. Check the Kafka topic

```bash
docker exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --list
```

*Result :*

```bash
earthquicks-cdc.public.earthquakes
```
---

### 7. Start a Kafka consumer

#### Run:

```bash
docker exec -it kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic earthquicks-cdc.public.earthquakes `
  --from-beginning
```
---

### 8. Insert a test record into PostgreSQL

#### 8.1 Connect:

```bash
docker exec -it postgres psql -U postgres -d maritime_logistics
```

#### 8.2 Then insert:

```sql
INSERT INTO earthquakes (
    unid,
    source_id,
    source_catalog,
    lastupdate,
    time,
    flynn_region,
    lat,
    lon,
    depth,
    evtype,
    auth,
    mag,
    magtype,
    action
)
VALUES (
    'CDC-TEST-001',
    'test-source-001',
    'TEST',
    NOW(),
    NOW(),
    'Egypt Test Region',
    29.9792,
    31.1342,
    10.5,
    'earthquake',
    'TEST',
    5.5,
    'ML',
    'CREATE'
);
```
---

### 9. Watch the Kafka terminal

- If everything is working, your Kafka consumer should immediately receive an event.

```json
{
  "before": null,
  "after": {
    "unid": "CDC-TEST-001",
    "source_id": "test-source-001",
    "source_catalog": "TEST",
    "lastupdate": "...",
    "time": "...",
    "flynn_region": "Egypt Test Region",
    "lat": 29.9792,
    "lon": 31.1342,
    "depth": 10.5,
    "evtype": "earthquake",
    "auth": "TEST",
    "mag": 5.5,
    "magtype": "ML",
    "action": "CREATE",
    "received_at": "..."
  },
  "op": "c"
}
```

##### The important part is:

- "op": "c"

- which means: CREATE / INSERT

### 10. Test UPDATE

- Go back to PostgreSQL:

```sql
UPDATE earthquakes
SET magnitude = 6.1
WHERE location = 'Test Location';
```


- This time: "op": "u"

- means: UPDATE

### 11. Test DELETE

- Then:

```sql
DELETE FROM earthquakes
WHERE location = 'Test Location';
```

- Kafka should receive: "op": "d"

- meaning: DELETE

---

## Resource Links

- What in `connector.json` file : [Debezium properties](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-example-configuration)

- Why Avro Serialization : [Avro Serialization](https://debezium.io/documentation/reference/stable/configuration/avro.html)

