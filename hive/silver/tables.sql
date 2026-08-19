CREATE DATABASE IF NOT EXISTS tidaline_silver;
USE tidaline_silver;

-- =====================================================================
-- SILVER_PORTS
-- Cleaned and validated ports data from Bronze layer
-- Business logic applied for supplies_rate and comm_rate
-- =====================================================================
CREATE TABLE IF NOT EXISTS Silver_Ports (
    world_port_index_number  INT COMMENT 'Natural/business key from NGA source',
    main_port_name           VARCHAR(200),
    alternate_port_name      VARCHAR(200),
    un_locode               VARCHAR(10),
    country_code            VARCHAR(10),
    region_name             VARCHAR(100),
    world_water_body        VARCHAR(100),
    harbor_size             VARCHAR(50),
    harbor_type             VARCHAR(50),
    harbor_use              VARCHAR(50),
    shelter_afforded        VARCHAR(50),
    latitude                DECIMAL(9,6),
    longitude               DECIMAL(9,6),
    tidal_range_m           DECIMAL(5,1),
    entrance_width_m        DECIMAL(5,1),
    channel_depth_m         DECIMAL(5,1),
    anchorage_depth_m       DECIMAL(5,1),
    cargo_pier_depth_m      DECIMAL(5,1),
    oil_terminal_depth_m    DECIMAL(5,1),
    lng_terminal_depth_m    DECIMAL(5,1),
    max_vessel_length_m     DECIMAL(6,1),
    max_vessel_beam_m       DECIMAL(6,1),
    max_vessel_draft_m      DECIMAL(6,1),
    
    -- Business logic fields
    supplies_rate           VARCHAR(20) COMMENT 'Excellent/Good/Limited/Unavailable based on Provisions/Fuel Oil/Diesel/Potable Water/Repairs',
    comm_rate               VARCHAR(20) COMMENT 'Excellent/Good/Limited/Unavailable based on Radio/telephone/airport/Telefax',
    
    -- Raw service flags for business logic
    supplies_provisions     BOOLEAN,
    supplies_potable_water  BOOLEAN,
    supplies_fuel_oil       BOOLEAN,
    supplies_diesel_oil     BOOLEAN,
    supplies_aviation_fuel  BOOLEAN,
    supplies_deck           BOOLEAN,
    supplies_engine         BOOLEAN,
    repairs_available      BOOLEAN,
    
    comm_telephone         BOOLEAN,
    comm_telefax           BOOLEAN,
    comm_radio             BOOLEAN,
    comm_airport           BOOLEAN,
    comm_rail              BOOLEAN,
    
    -- Metadata
    bronze_file_name       STRING COMMENT 'Source file from Bronze layer',
    bronze_load_date       DATE COMMENT 'When Bronze layer loaded this file',
    silver_process_date    TIMESTAMP COMMENT 'When Silver layer processed this record',
    etl_load_date          TIMESTAMP COMMENT 'ETL load timestamp'
)
PARTITIONED BY (load_date STRING COMMENT 'Partition by load date for efficient querying')
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
