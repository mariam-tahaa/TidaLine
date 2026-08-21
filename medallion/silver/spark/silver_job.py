from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date, regexp_extract,
    coalesce, upper, trim, abs, row_number
)
from pyspark.sql.types import IntegerType, DecimalType, BooleanType, StringType
from pyspark.sql.window import Window

import os
import re
import sys
from datetime import datetime

# Add current directory to path for logger import
sys.path.insert(0, "/opt/spark-apps")
from logger import Logger


# =============================================================================
# 1. Logger Initialization
# =============================================================================

logger = Logger(log_file="/tmp/silver_ports_etl.log")
logger.info("Silver Ports ETL process started")


# =============================================================================
# Helper Functions
# =============================================================================

def clean_coordinates(df, lat_col, lon_col):
    """
    Clean latitude and longitude columns by setting invalid values to NULL.
    Valid ranges: Latitude [-90, 90], Longitude [-180, 180]
    """
    df = df.withColumn(
        lat_col,
        when((col(lat_col) >= -90) & (col(lat_col) <= 90), col(lat_col))
        .otherwise(None)
    )
    df = df.withColumn(
        lon_col,
        when((col(lon_col) >= -180) & (col(lon_col) <= 180), col(lon_col))
        .otherwise(None)
    )
    return df


def fill_unknown_strings(df, string_cols):
    """
    Trim string columns and replace NULL, empty, or whitespace-only
    values with 'UNKNOWN'.
    """
    for col_name in string_cols:
        df = df.withColumn(
            col_name,
            when(
                col(col_name).isNull() |
                (trim(col(col_name)) == ""),
                lit("UNKNOWN")
            ).otherwise(trim(col(col_name)))
        )
    return df


def deduplicate_by_key(df, key_cols, order_col):
    """
    Deduplicate DataFrame by key columns, keeping the record with the latest order_col value.
    """
    window_spec = Window.partitionBy(*key_cols).orderBy(col(order_col).desc())
    return (
        df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )


# =============================================================================
# 2. Spark Session
# =============================================================================

try:
    spark = (
        SparkSession.builder
        .appName("BronzeToSilverPorts")
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate()
    )
    logger.info("Spark session created successfully")
except Exception as e:
    logger.error("Failed to create Spark session: %s", str(e))
    raise


# =============================================================================
# 3. Configuration
# =============================================================================

BRONZE_PATH = "/bronze/ports"  # HDFS path for Bronze layer
SILVER_PATH = "hdfs://namenode:8020/silver/ports"
SILVER_TABLE = "tidaline_silver.Silver_Ports"
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")


# =============================================================================
# 4. Read Bronze Layer Data
# =============================================================================

try:
    bronze_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(BRONZE_PATH)
    )
    logger.info("Bronze data loaded successfully from %s", BRONZE_PATH)
    logger.info("Initial record count: %d", bronze_df.count())
except Exception as e:
    logger.error("Failed to load bronze data: %s", str(e))
    raise


# =============================================================================
# 5. Data Cleaning and Transformation
# =============================================================================

silver_df = (
    bronze_df
    
    # Select and rename relevant columns
    .select(
        col("World Port Index Number").alias("world_port_index_number"),
        col("Main Port Name").alias("main_port_name"),
        col("Alternate Port Name").alias("alternate_port_name"),
        col("UN/LOCODE").alias("un_locode"),
        col("Country Code").alias("country_code"),
        col("Region Name").alias("region_name"),
        col("World Water Body").alias("world_water_body"),
        col("Harbor Size").alias("harbor_size"),
        col("Harbor Type").alias("harbor_type"),
        col("Harbor Use").alias("harbor_use"),
        col("Shelter Afforded").alias("shelter_afforded"),
        col("Latitude").alias("latitude"),
        col("Longitude").alias("longitude"),
        col("Tidal Range (m)").alias("tidal_range_m"),
        col("Entrance Width (m)").alias("entrance_width_m"),
        col("Channel Depth (m)").alias("channel_depth_m"),
        col("Anchorage Depth (m)").alias("anchorage_depth_m"),
        col("Cargo Pier Depth (m)").alias("cargo_pier_depth_m"),
        col("Oil Terminal Depth (m)").alias("oil_terminal_depth_m"),
        col("Liquified Natural Gas Terminal Depth (m)").alias("lng_terminal_depth_m"),
        col("Maximum Vessel Length (m)").alias("max_vessel_length_m"),
        col("Maximum Vessel Beam (m)").alias("max_vessel_beam_m"),
        col("Maximum Vessel Draft (m)").alias("max_vessel_draft_m"),
        
        # Service flags - Supplies
        col("Supplies - Provisions").alias("supplies_provisions"),
        col("Supplies - Potable Water").alias("supplies_potable_water"),
        col("Supplies - Fuel Oil").alias("supplies_fuel_oil"),
        col("Supplies - Diesel Oil").alias("supplies_diesel_oil"),
        col("Repairs").alias("repairs"),
        
        # Service flags - Communications
        col("Communications - Telephone").alias("comm_telephone"),
        col("Communications - Telefax").alias("comm_telefax"),
        col("Communications - Radio").alias("comm_radio"),
        col("Communications - Airport").alias("comm_airport")
    )
    
    # Clean string columns - trim and standardize
    .withColumn("main_port_name", trim(col("main_port_name")))
    .withColumn("alternate_port_name", trim(col("alternate_port_name")))
    .withColumn("un_locode", upper(trim(col("un_locode"))))
    .withColumn("country_code", upper(trim(col("country_code"))))
    .withColumn("region_name", trim(col("region_name")))
    .withColumn("harbor_size", trim(col("harbor_size")))
    .withColumn("harbor_type", trim(col("harbor_type")))
    .withColumn("harbor_use", trim(col("harbor_use")))
    .withColumn("shelter_afforded", trim(col("shelter_afforded")))
    
    # Convert Yes/No strings to Boolean
    # Yes → True, No → False, Unknown/NULL → NULL
    .withColumn("supplies_provisions", 
                 when(col("supplies_provisions") == "Yes", True)
                 .when(col("supplies_provisions") == "No", False)
                .otherwise(None))
    .withColumn("supplies_potable_water", 
                 when(col("supplies_potable_water") == "Yes", True)
                 .when(col("supplies_potable_water") == "No", False)
                 .otherwise(None))
    .withColumn("supplies_fuel_oil", 
                 when(col("supplies_fuel_oil") == "Yes", True)
                 .when(col("supplies_fuel_oil") == "No", False)
                 .otherwise(None))
    .withColumn("supplies_diesel_oil", 
                 when(col("supplies_diesel_oil") == "Yes", True)
                 .when(col("supplies_diesel_oil") == "No", False)
                 .otherwise(None))
    # Major/Moderate/Limited → True
    # Emergency Only/Unknown/NULL → False
    .withColumn("repairs",
                 when(col("repairs").isin("Major", "Moderate", "Limited"), True)
                 .otherwise(False)
)
    
    .withColumn("comm_telephone", 
                 when(col("comm_telephone") == "Yes", True)
                 .when(col("comm_telephone") == "No", False)
                 .otherwise(None))
    .withColumn("comm_telefax", 
                 when(col("comm_telefax") == "Yes", True)
                 .when(col("comm_telefax") == "No", False)
                 .otherwise(None))
    .withColumn("comm_radio", 
                 when(col("comm_radio") == "Yes", True)
                 .when(col("comm_radio") == "No", False)
                 .otherwise(None))
    .withColumn("comm_airport", 
                 when(col("comm_airport") == "Yes", True)
                 .when(col("comm_airport") == "No", False)
                 .otherwise(None))
    
    # Calculate supplies_rate based on business logic
    # Count of Provisions/Fuel Oil/Diesel/Potable Water/Repairs
    .withColumn(
        "supplies_count",
        coalesce(col("supplies_provisions").cast(IntegerType()), lit(0)) +
        coalesce(col("supplies_potable_water").cast(IntegerType()), lit(0)) +
        coalesce(col("supplies_fuel_oil").cast(IntegerType()), lit(0)) +
        coalesce(col("supplies_diesel_oil").cast(IntegerType()), lit(0)) +
        coalesce(col("repairs").cast(IntegerType()), lit(0))
    )
    .withColumn(
        "supplies_rate",
        when(col("supplies_count") == 5, "Excellent")
        .when(col("supplies_count") >= 3, "Good")
        .when(col("supplies_count") >= 1, "Limited")
        .otherwise("Unavailable")
    )
    
    # Calculate comm_rate based on business logic
    # Count of Radio/telephone/airport/Telefax
    .withColumn(
        "comm_count",
        coalesce(col("comm_radio").cast(IntegerType()), lit(0)) +
        coalesce(col("comm_telephone").cast(IntegerType()), lit(0)) +
        coalesce(col("comm_airport").cast(IntegerType()), lit(0)) +
        coalesce(col("comm_telefax").cast(IntegerType()), lit(0))
    )
    .withColumn(
        "comm_rate",
        when(col("comm_count") == 4, "Excellent")
        .when(col("comm_count") >= 2, "Good")
        .when(col("comm_count") >= 1, "Limited")
        .otherwise("Unavailable")
    )
    
    # Add metadata columns
    .withColumn("bronze_file_name", lit("ports_2026-08-01.csv"))  # Extract from input path in production
    .withColumn("bronze_load_date", lit("2026-08-01").cast("date"))
    .withColumn("silver_process_date", current_timestamp())
    .withColumn("etl_load_date", current_timestamp())
    .withColumn("load_date", lit(LOAD_DATE))
    
    # Drop temporary columns
    .drop("supplies_count", "comm_count")
    
    # Filter out invalid records
    .filter(col("world_port_index_number").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())
)

logger.info("Column selection and basic transformation completed")


# =============================================================================
# 6. Data Validation and Cleaning
# =============================================================================

# Clean coordinates - validate ranges
silver_df = clean_coordinates(silver_df, "latitude", "longitude")
logger.info("Coordinate validation completed")

# Fill NULL string columns with 'UNKNOWN'
string_cols = [
    "main_port_name", "alternate_port_name", "un_locode", "country_code",
    "region_name", "world_water_body", "harbor_size", "harbor_type",
    "harbor_use", "shelter_afforded"
]
silver_df = fill_unknown_strings(silver_df, string_cols)
logger.info("String columns filled with UNKNOWN for NULL values")

# Deduplicate by port index (keep latest based on processing timestamp)
silver_df = deduplicate_by_key(silver_df, ["world_port_index_number"], "silver_process_date")
logger.info("Deduplication completed by world_port_index_number")

# Final validation - ensure critical fields are not NULL after cleaning
critical_fields = ["world_port_index_number", "main_port_name", "country_code", "latitude", "longitude"]
before_count = silver_df.count()
silver_df = silver_df.filter(
    col("world_port_index_number").isNotNull() &
    col("latitude").isNotNull() &
    col("longitude").isNotNull()
)
after_count = silver_df.count()
logger.info("Final validation: %d records removed due to missing critical fields", before_count - after_count)


# =============================================================================
# 7. Write to Silver Layer
# =============================================================================

try:
    (
        silver_df.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("load_date")
        .save(SILVER_PATH)

    )

    # Register newly created partitions in Hive Metastore
    spark.sql(f"MSCK REPAIR TABLE {SILVER_TABLE}")

    logger.info("Successfully loaded %d records to %s", silver_df.count(), SILVER_TABLE)
    logger.info("Data written to partition: load_date=%s", LOAD_DATE)
except Exception as e:
    logger.error("Failed to write data to Silver layer: %s", str(e))
    raise
finally:
    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Silver Ports ETL process completed successfully")
