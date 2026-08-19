CREATE DATABASE IF NOT EXISTS tidaline_gold;
USE tidaline_gold;

-- =====================================================================
-- DIM_DATE
-- =====================================================================
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key       INT COMMENT 'Surrogate key, format YYYYMMDD',
    full_date      DATE NOT NULL,
    day_num        INT,
    month_num      INT,
    month_name     VARCHAR(15),
    quarter_num    INT,
    year_num       INT,
    day_of_week    VARCHAR(10),
    PRIMARY KEY (date_key) DISABLE NOVALIDATE RELY,
    UNIQUE (full_date) DISABLE NOVALIDATE RELY
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- =====================================================================
-- DIM_PORT  (SCD Type 2)
-- =====================================================================
CREATE TABLE IF NOT EXISTS Dim_Port (
    port_key                 INT COMMENT 'Surrogate key',
    world_port_index_number  INT COMMENT 'Natural/business key from NGA source',
    main_port_name           VARCHAR(200),
    harbor_size               VARCHAR(50),
    harbor_type               VARCHAR(50),
    harbor_use                VARCHAR(50),
    country_code              VARCHAR(10),
    region_name                VARCHAR(100),
    latitude                  DECIMAL(9,6),
    longitude                 DECIMAL(9,6),
    shelter_afforded          VARCHAR(50),
    supplies_rate             VARCHAR(20),   
    comm_rate                 VARCHAR(20),  
    channel_depth             DECIMAL(5,1),
    anchorage_depth           DECIMAL(5,1),
    cargo_pier_depth          DECIMAL(5,1),
    oil_terminal_depth        DECIMAL(5,1),
    LNG_terminal_depth        DECIMAL(5,1),
    effective_date            INT COMMENT 'FK -> Dim_Date.date_key, SCD2 validity start',
    end_date                  INT COMMENT 'FK -> Dim_Date.date_key, SCD2 validity end (NULL/9999999 if current)',
    is_current                BOOLEAN,
    etl_load_date             TIMESTAMP,
    PRIMARY KEY (port_key) DISABLE NOVALIDATE RELY,
    CONSTRAINT chk_supplies_rate CHECK (supplies_rate IN ('Excellent','Good','Limited','Unavailable')) DISABLE NOVALIDATE,
    CONSTRAINT chk_comm_rate     CHECK (comm_rate     IN ('Excellent','Good','Limited','Unavailable')) DISABLE NOVALIDATE,
    CONSTRAINT fk_port_effdate FOREIGN KEY (effective_date) REFERENCES Dim_Date(date_key) DISABLE NOVALIDATE RELY,
    CONSTRAINT fk_port_enddate FOREIGN KEY (end_date)       REFERENCES Dim_Date(date_key) DISABLE NOVALIDATE RELY
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- =====================================================================
-- FACT_SEISMIC_EVENT
-- Grain: one row per earthquake.
-- =====================================================================
CREATE TABLE IF NOT EXISTS Fact_Seismic_Event (
    seismic_sk       INT COMMENT 'Surrogate key',
    unid             STRING COMMENT 'Natural key from seismic monitoring service',
    source_id        INT,
    source_catalog   VARCHAR(50),
    evtype           VARCHAR(50),
    auth             VARCHAR(100),
    flynn_region     VARCHAR(150),
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    mag              DECIMAL(4,2),
    magtype          VARCHAR(10),
    depth_km         DECIMAL(6,2),
    event_time       TIMESTAMP COMMENT 'When the earthquake actually occurred (business time)',
    action           VARCHAR(10) COMMENT 'Action flag from source Seismic Events API',
    received_at      TIMESTAMP COMMENT 'When the pipeline ingested this record (operational metadata)',
    etl_load_date    TIMESTAMP,
    PRIMARY KEY (seismic_sk) DISABLE NOVALIDATE RELY
)
PARTITIONED BY (date_key INT COMMENT 'Derived from event_time, format YYYYMMDD')
CLUSTERED BY (seismic_sk) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- =====================================================================
-- FACT_PORT_SEISMIC_PROXIMITY  (bridge / factless fact)
-- Grain: one row per (port, earthquake)
-- =====================================================================
CREATE TABLE IF NOT EXISTS Fact_Port_Seismic_Proximity (
    port_seismic_sk  INT COMMENT 'Surrogate key',
    seismic_sk       INT COMMENT 'FK -> Fact_Seismic_Event.seismic_sk',
    port_key         INT COMMENT 'FK -> Dim_Port.port_key',
    distance_km      DECIMAL(8,2),
    within_50km      BOOLEAN,
    within_100km     BOOLEAN,
    etl_load_date    TIMESTAMP,
    PRIMARY KEY (port_seismic_sk) DISABLE NOVALIDATE RELY,
    CONSTRAINT fk_prox_seismic FOREIGN KEY (seismic_sk) REFERENCES Fact_Seismic_Event(seismic_sk) DISABLE NOVALIDATE RELY,
    CONSTRAINT fk_prox_port    FOREIGN KEY (port_key)    REFERENCES Dim_Port(port_key)             DISABLE NOVALIDATE RELY
)
PARTITIONED BY (date_key INT COMMENT 'Denormalized from the associated event''s date')
CLUSTERED BY (port_key) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- =====================================================================
-- FACT_PORT_RISK_SNAPSHOT  (periodic snapshot fact)
-- Grain: one row per (port, date)
-- =====================================================================
CREATE TABLE IF NOT EXISTS Fact_Port_Risk_Snapshot (
    port_risk_sk           INT COMMENT 'Surrogate key',
    port_key               INT COMMENT 'FK -> Dim_Port.port_key',
    nearest_event_key      INT COMMENT 'FK -> Fact_Seismic_Event.seismic_sk',
    nearest_distance_km    DECIMAL(8,2),
    classification         VARCHAR(20), 
    etl_load_date          TIMESTAMP,
    PRIMARY KEY (port_risk_sk) DISABLE NOVALIDATE RELY,
    CONSTRAINT chk_classification CHECK (classification IN ('safe','cautionary','dangerous')) DISABLE NOVALIDATE,
    CONSTRAINT fk_snap_port  FOREIGN KEY (port_key)          REFERENCES Dim_Port(port_key)             DISABLE NOVALIDATE RELY,
    CONSTRAINT fk_snap_event FOREIGN KEY (nearest_event_key) REFERENCES Fact_Seismic_Event(seismic_sk) DISABLE NOVALIDATE RELY,
    CONSTRAINT uq_port_date  UNIQUE (port_key, date_key) DISABLE NOVALIDATE RELY 
)
PARTITIONED BY (date_key INT)
CLUSTERED BY (port_key) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');