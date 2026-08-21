CREATE DATABASE IF NOT EXISTS tidaline_gold;
USE tidaline_gold;

-- =====================================================================
-- DIM_DATE
-- =====================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Date (
    date_key       INT COMMENT 'Surrogate key, format YYYYMMDD',
    full_date      DATE,
    day_num        INT,
    month_num      INT,
    month_name     STRING,
    quarter_num    INT,
    year_num       INT,
    day_of_week    STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/gold/dim_date'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =====================================================================
-- DIM_PORT  (SCD Type 2)
-- =====================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Port (
    port_key                 INT COMMENT 'Surrogate key',
    world_port_index_number  INT COMMENT 'Natural/business key from NGA source',
    main_port_name           STRING,
    harbor_size              STRING,
    harbor_type              STRING,
    harbor_use               STRING,
    country_code             STRING,
    region_name              STRING,
    latitude                 DECIMAL(9,6),
    longitude                DECIMAL(9,6),
    shelter_afforded         STRING,
    supplies_rate            STRING,
    comm_rate                STRING,
    channel_depth            DECIMAL(5,1),
    anchorage_depth          DECIMAL(5,1),
    cargo_pier_depth         DECIMAL(5,1),
    oil_terminal_depth       DECIMAL(5,1),
    LNG_terminal_depth       DECIMAL(5,1),
    effective_date           INT COMMENT 'FK -> Dim_Date.date_key, SCD2 validity start',
    end_date                 INT COMMENT 'FK -> Dim_Date.date_key, SCD2 validity end (99991231 if current)',
    is_current               BOOLEAN,
    etl_load_date            TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/gold/dim_port'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =====================================================================
-- FACT_SEISMIC_EVENT
-- Grain: one row per earthquake.
-- =====================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS Fact_Seismic_Event (
    seismic_sk       INT COMMENT 'Surrogate key',
    unid             STRING COMMENT 'Natural key from seismic monitoring service',
    source_id        STRING,
    source_catalog   STRING,
    evtype           STRING,
    auth             STRING,
    flynn_region     STRING,
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    mag              DECIMAL(4,2),
    magtype          STRING,
    depth_km         DECIMAL(6,2),
    event_time       TIMESTAMP COMMENT 'When the earthquake actually occurred',
    action           STRING COMMENT 'Action flag from source Seismic Events API',
    received_at      TIMESTAMP COMMENT 'When the pipeline ingested this record',
    etl_load_date    TIMESTAMP
)
PARTITIONED BY (date_key INT COMMENT 'Derived from event_time, format YYYYMMDD')
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/gold/fact_seismic_event'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =====================================================================
-- FACT_PORT_SEISMIC_PROXIMITY  (bridge / factless fact)
-- Grain: one row per (port, earthquake)
-- =====================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS Fact_Port_Seismic_Proximity (
    port_seismic_sk  INT COMMENT 'Surrogate key',
    seismic_sk       INT COMMENT 'FK -> Fact_Seismic_Event.seismic_sk',
    port_key         INT COMMENT 'FK -> Dim_Port.port_key',
    distance_km      DECIMAL(8,2),
    within_50km      BOOLEAN,
    within_100km     BOOLEAN,
    etl_load_date    TIMESTAMP
)
PARTITIONED BY (date_key INT COMMENT 'Denormalized from the associated event date')
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/gold/fact_port_seismic_proximity'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =====================================================================
-- FACT_PORT_RISK_SNAPSHOT  (periodic snapshot fact)
-- Grain: one row per (port, date)
-- =====================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS Fact_Port_Risk_Snapshot (
    port_risk_sk           INT COMMENT 'Surrogate key',
    port_key               INT COMMENT 'FK -> Dim_Port.port_key',
    nearest_event_key      INT COMMENT 'FK -> Fact_Seismic_Event.seismic_sk',
    nearest_distance_km    DECIMAL(8,2),
    classification         STRING,
    etl_load_date          TIMESTAMP
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/gold/fact_port_risk_snapshot'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
