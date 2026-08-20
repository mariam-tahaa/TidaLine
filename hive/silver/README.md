# Silver Layer - Ports Data

## Overview
The Silver layer transforms raw Bronze data into cleaned, validated, and standardized data with business logic applied.

## What We Did

### 1. Schema Design
- Created `Silver_Ports` table in `tidaline_silver` database
- Includes all port attributes from Bronze layer
- Added business logic fields: `supplies_rate`, `comm_rate`
- Added metadata tracking: `bronze_file_name`, `bronze_load_date`, `silver_process_date`, `etl_load_date`
- Partitioned by `load_date` for efficient querying
- Stored as Parquet with SNAPPY compression
- - Hive external table points to the Silver HDFS directory:
  `hdfs://namenode:8020/silver/ports`

### 2. Business Logic
### supplies_rate:
Calculated from Provisions/Fuel Oil/Diesel/Potable Water/Repairs availability
 
| Available Services | Rate        |
|--------------------:|-------------|
| 5                    | Excellent   |
| 4                    | Good        |
| 2-3                  | Limited     |
| 0-1                  | Unavailable |
 
### comm_rate: 
Calculated from Radio/Telephone/Airport/Telefax availability
  
| Available Services | Rate        |
|--------------------:|-------------|
| 4                    | Excellent   |
| 3                    | Good        |
| 2                    | Limited     |
| 0-1                  | Unavailable |

### 3. Data Transformation
- Column selection and renaming
- String cleaning (trim, uppercase for codes)
- Yes/No to Boolean conversion
- Coordinate validation (Lat: -90 to 90, Lon: -180 to 180)
- NULL handling (fill string columns with "UNKNOWN")
- Deduplication by port index
- Validation filtering (remove records with missing critical fields)

## Data Flow

```
Bronze Layer (HDFS)
    ↓ ports CSV files
Silver Transformation (Spark Job)
    ↓ Cleaning + Validation + Business Logic
Silver Layer (HDFS)
    ↓ Parquet files
Hive External Table
    ↓ tidaline_silver.Silver_Ports
Gold Layer (Future)
```

## Files

- **`tables.sql`**: Hive schema definition for Silver_Ports table
- **`../medallion/silver/spark/silver_job.py`**: Spark transformation job

## Testing

### Create Silver HDFS Directory

Create the physical Silver directory in HDFS before creating the external Hive table:

```bash
docker exec namenode hdfs dfs -mkdir -p /silver/ports
docker exec namenode hdfs dfs -chmod -R 777 /silver
```

### 1. Create Silver External Hive Table
```bash
docker cp hive/silver/tables.sql spark:/tmp/tables.sql
docker exec spark /opt/spark/bin/spark-sql -f /tmp/tables.sql
```

### 2. Run Spark Transformation Job
```bash
# Copy job and logger to container
docker cp medallion/silver/spark/silver_job.py spark:/opt/spark-apps/silver_job.py
docker cp utils/logger.py spark:/opt/spark-apps/logger.py

# Execute job
docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/silver_job.py
```

### 3. Verify Results
```bash
# Check record count
docker exec spark /opt/spark/bin/spark-sql -e "SELECT COUNT(*) FROM tidaline_silver.Silver_Ports WHERE load_date = '2026-08-19';"

# Check data quality (should be 0)
docker exec spark /opt/spark/bin/spark-sql -e "SELECT COUNT(*) FROM tidaline_silver.Silver_Ports WHERE latitude IS NULL OR longitude IS NULL;"

# Check business logic distribution
docker exec spark /opt/spark/bin/spark-sql -e "SELECT supplies_rate, COUNT(*) FROM tidaline_silver.Silver_Ports GROUP BY supplies_rate;"

# Verify schema
docker exec spark /opt/spark/bin/spark-sql -e "DESCRIBE tidaline_silver.Silver_Ports;"

# Check logs
docker exec spark cat /tmp/silver_ports_etl.log
```

### 4. Verify HDFS Output
Check the Silver HDFS directory:
 
```bash
docker exec namenode hdfs dfs -ls -R /silver/ports
```

## Test Results
- **Records Processed**: 3,803 ports
- **Data Quality**: 0 NULL values in critical fields
- **Business Logic**: Supplies rate distribution (Good: 1,177, Limited: 1,611, Unavailable: 1,015)
- **Partitioning**: Working correctly with load_date partition

## Key Features
- Enterprise-grade logging and error handling
- Data validation and quality checks
- Deduplication logic
- Coordinate range validation
- Metadata tracking for audit trail
- - HDFS-based Silver layer
- Hive external table for analytics access

## Important Note
 
The Silver HDFS directory and Hive external table are separate components:
 
- `/silver/ports` is the physical storage location in HDFS.
- `tidaline_silver.Silver_Ports` is the Hive metadata layer.
- The Hive external table points to `/silver/ports`.
- The Spark job writes the actual Parquet data into `/silver/ports`.
