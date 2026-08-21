# Gold Layer - Ports & Earthquakes

## Overview
The Gold layer turns cleaned Silver/Bronze data into a **dimensional model** for analytics and dashboards. It answers three business questions:
- What ports do we know about, and how have they changed over time?
- What earthquakes happened?
- Which ports are close enough to recent earthquakes to be at risk?

## What We Did

### 1. Schema Design (`tables.sql`)
Created the `tidaline_gold` database with five Hive **external** tables. Each table points to Parquet files on HDFS (`hdfs://namenode:8020/gold/...`).

#### Dim_Date
- Standard **date dimension** used as a lookup for SCD2 validity dates and fact partitions.
- One row per calendar day from **2020-01-01 to 2030-12-31**.
- `date_key` = integer `YYYYMMDD` (e.g. `20260820`).
- Includes day, month, quarter, year, and day-of-week for reporting.

#### Dim_Port (SCD Type 2)
- **Port dimension** — the main analytical view of ports.
- `world_port_index_number` = natural/business key from the NGA source.
- `port_key` = surrogate key assigned by the Gold job.
- Stores port attributes: name, location, harbor info, `supplies_rate`, `comm_rate`, water depths.
- **SCD Type 2 columns:**
  - `effective_date` — when this version became valid
  - `end_date` — when it stopped being valid (`99991231` = still current)
  - `is_current` — `true` for the active version of each port
- Lets us report on port history: if harbor size or supplies rate changes, we keep the old row and add a new one.

#### Fact_Seismic_Event
- **Earthquake fact table** — one row per earthquake event.
- `unid` = natural key from the seismic API source.
- `seismic_sk` = surrogate key.
- Stores magnitude, depth, coordinates, region, event time, and ingestion metadata.
- **Partitioned by `date_key`** (derived from `event_time`) for efficient date-range queries.

#### Fact_Port_Seismic_Proximity
- **Bridge table** linking ports to earthquakes.
- One row per **(port, earthquake)** pair.
- `distance_km` = great-circle distance computed with the **Haversine formula**.
- `within_50km` / `within_100km` = boolean flags for proximity bands.
- Partitioned by the earthquake's event date.

#### Fact_Port_Risk_Snapshot
- **Periodic snapshot** — one row per port per snapshot date.
- For each port, finds the **nearest earthquake** and assigns a risk class:
  - **dangerous** — nearest event ≤ 50 km
  - **cautionary** — nearest event > 50 km and ≤ 100 km
  - **safe** — nearest event > 100 km
- Links to `Dim_Port` via `port_key` and to `Fact_Seismic_Event` via `nearest_event_key`.

---

### 2. Gold Ports Job (`gold_ports_job.py`)

**Input:** latest partition of `tidaline_silver.Silver_Ports`  
**Output:** `Dim_Date`, `Dim_Port` on HDFS

What the job does step by step:

1. **Build Dim_Date** — generates every date in the 2020–2030 range with calendar attributes.
2. **Read Silver** — picks the latest `load_date` from `Silver_Ports` (e.g. `2026-08-19`).
3. **Map columns** — renames Silver fields to Gold names (e.g. `channel_depth_m` → `channel_depth`, `lng_terminal_depth_m` → `LNG_terminal_depth`).
4. **Apply SCD Type 2:**
   - **New port** (not in Dim_Port) → insert with `is_current = true`, `end_date = 99991231`
   - **Changed port** (tracked columns differ) → expire old row (`is_current = false`, set `end_date` to today) and insert new current row
   - **Unchanged port** → keep existing row as-is
5. **Write Parquet** to `/gold/dim_date` and `/gold/dim_port`.

**SCD2 change detection** uses `SCD_COMPARE_COLS` only:
`harbor_size`, `harbor_use`, `shelter_afforded`, `supplies_rate`, `comm_rate`, and the five depth fields.

Name, country, and coordinates are stored in Dim_Port but do **not** trigger a new SCD2 version with the current config.

---

### 3. Gold Earthquakes Job (`gold_earthquakes_job.py`)

**Input:** Bronze HDFS (`/bronze/earthquakes`) with PostgreSQL fallback  
**Also reads:** current rows from `Dim_Port` (must run **after** Gold Ports)  
**Output:** `Fact_Seismic_Event`, `Fact_Port_Seismic_Proximity`, `Fact_Port_Risk_Snapshot`

What the job does step by step:

1. **Load earthquakes** — attempts to read CSV from Bronze HDFS at `/bronze/earthquakes`. If HDFS read fails or is empty, falls back to PostgreSQL database `maritime_logistics.earthquakes`.
2. **Build Fact_Seismic_Event** — maps source columns (`lat`/`lon`/`time`/`depth`/`mag`) to Gold schema, assigns `seismic_sk`, derives `date_key` from event time, writes partitioned Parquet.
3. **Compute proximity** — cross-joins every **current** port with every earthquake, calculates Haversine distance in km, sets `within_50km` and `within_100km` flags.
4. **Build risk snapshot** — for each port, picks the closest earthquake and assigns `classification` (dangerous / cautionary / safe).
5. **Write Parquet** to `/gold/fact_seismic_event`, `/gold/fact_port_seismic_proximity`, `/gold/fact_port_risk_snapshot`.

**Config constants** at the top of the job (HDFS paths) are normal for this Docker setup — same pattern as the Silver job.

---

## Data Flow

```
Silver Layer (HDFS + Hive)
    ↓  Silver_Ports
Gold Ports Job (Spark)
    ↓  Dim_Date + Dim_Port (SCD2)
Bronze Layer (HDFS)
    ↓  earthquake CSV files
Gold Earthquakes Job (Spark)
    ↓  Fact_Seismic_Event
    ↓  Fact_Port_Seismic_Proximity  (Haversine distance)
    ↓  Fact_Port_Risk_Snapshot      (risk classification)
Hive External Tables
    ↓  tidaline_gold.*
Dashboards / Analytics
```

**Run order:** Silver → Gold Ports → Gold Earthquakes

## Files

- **`tables.sql`**: Hive schema for all five Gold tables
- **`../medallion/gold/spark/gold_ports_job.py`**: Ports → Dim_Date, Dim_Port
- **`../medallion/gold/spark/gold_earthquakes_job.py`**: Earthquakes → facts + risk

## Testing

### 1. Create Gold HDFS directories
```bash
docker exec namenode hdfs dfs -mkdir -p /gold/dim_date /gold/dim_port /gold/fact_seismic_event /gold/fact_port_seismic_proximity /gold/fact_port_risk_snapshot
docker exec namenode hdfs dfs -chmod -R 777 /gold
```

### 2. Create Gold Hive tables
```bash
docker cp hive/gold/tables.sql spark:/tmp/gold_tables.sql
docker exec spark /opt/spark/bin/spark-sql -f /tmp/gold_tables.sql
docker exec spark /opt/spark/bin/spark-sql -e "SHOW TABLES IN tidaline_gold;"
```

### 3. Run Gold jobs (Silver must exist first)
```bash
docker cp medallion/gold/spark/gold_ports_job.py spark:/opt/spark-apps/gold_ports_job.py
docker cp medallion/gold/spark/gold_earthquakes_job.py spark:/opt/spark-apps/gold_earthquakes_job.py

docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/gold_ports_job.py
docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/gold_earthquakes_job.py
```

### 4. Repair partitions & verify
```bash
docker exec spark /opt/spark/bin/spark-sql -e "SELECT COUNT(*) FROM tidaline_gold.Dim_Port WHERE is_current=true;"
docker exec spark /opt/spark/bin/spark-sql -e "SELECT COUNT(*) FROM tidaline_gold.Fact_Seismic_Event;"
docker exec spark /opt/spark/bin/spark-sql -e "SELECT classification, COUNT(*) FROM tidaline_gold.Fact_Port_Risk_Snapshot GROUP BY classification;"

docker exec spark cat /tmp/gold_ports_etl.log
docker exec spark cat /tmp/gold_earthquakes_etl.log
```

### 5. Verify HDFS output
```bash
docker exec namenode hdfs dfs -ls -R /gold
```

## Test Results
- **Dim_Date**: 4,018 rows (full 2020–2030 calendar)
- **Dim_Port (current)**: 3,803 ports from Silver
- **Fact_Seismic_Event**: 414 earthquakes (with date partitions)
- **Fact_Port_Seismic_Proximity**: 1,574,442 pairs (3,803 ports × 414 events)
- **Risk classification**: 3,803 ports classified

## Important Notes

- `/gold/...` on HDFS holds the Parquet files; `tidaline_gold.*` Hive tables are the query layer on top — same pattern as Silver.
- Gold Earthquakes depends on `Dim_Port` — always run Gold Ports first.
