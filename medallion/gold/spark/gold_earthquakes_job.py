"""
Gold Earthquakes ETL: Bronze -> Fact_Seismic_Event + proximity + risk snapshot.

Reads earthquake CSV from Bronze HDFS, builds the seismic fact table,
computes port-earthquake proximity (Haversine), and derives per-port
risk classifications for the snapshot date.
"""

from datetime import datetime
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    asin, col, cos, date_format, lit, radians,
    row_number, sin, sqrt, to_timestamp, when,
)
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

sys.path.insert(0, "/opt/spark-apps")
from logger import Logger


logger = Logger(log_file="/tmp/gold_earthquakes_etl.log")
logger.info("Gold Earthquakes ETL process started")

# Pipeline paths (Docker Compose HDFS — same pattern as Silver)
BRONZE_PATH = "hdfs://namenode:8020/bronze/earthquakes"
GOLD_FACT_SEISMIC_PATH = "hdfs://namenode:8020/gold/fact_seismic_event"
GOLD_PROXIMITY_PATH = "hdfs://namenode:8020/gold/fact_port_seismic_proximity"
GOLD_RISK_SNAPSHOT_PATH = "hdfs://namenode:8020/gold/fact_port_risk_snapshot"
GOLD_DIM_PORT_PATH = "hdfs://namenode:8020/gold/dim_port"

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres."""
    lat1_r = radians(lat1)
    lon1_r = radians(lon1)
    lat2_r = radians(lat2)
    lon2_r = radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = sin(dlat / 2) ** 2 + cos(lat1_r) * cos(lat2_r) * sin(dlon / 2) ** 2
    a_clamped = when(a > 1, lit(1.0)).when(a < 0, lit(0.0)).otherwise(a)
    return lit(EARTH_RADIUS_KM) * 2 * asin(sqrt(a_clamped))


def read_bronze_earthquakes(spark):
    """Load earthquake CSV from Bronze HDFS."""
    bronze_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(BRONZE_PATH)
    )
    logger.info("Loaded earthquakes from Bronze HDFS: %s", BRONZE_PATH)
    return bronze_df


def build_fact_seismic_event(bronze_df, etl_ts):
    """Transform bronze rows into Fact_Seismic_Event layout."""
    events = (
        bronze_df
        .filter(col("unid").isNotNull())
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn(
            "date_key",
            date_format(col("event_time"), "yyyyMMdd").cast(IntegerType()),
        )
        .filter(col("date_key").isNotNull())
    )

    w = Window.orderBy("unid")
    return (
        events
        .withColumn("seismic_sk", row_number().over(w).cast(IntegerType()))
        .select(
            col("seismic_sk"),
            col("unid"),
            col("source_id").cast(StringType()).alias("source_id"),
            col("source_catalog"),
            col("evtype"),
            col("auth"),
            col("flynn_region"),
            col("lat").cast(DecimalType(9, 6)).alias("latitude"),
            col("lon").cast(DecimalType(9, 6)).alias("longitude"),
            col("mag").cast(DecimalType(4, 2)).alias("mag"),
            col("magtype"),
            col("depth").cast(DecimalType(6, 2)).alias("depth_km"),
            col("event_time"),
            col("action"),
            col("received_at").cast(TimestampType()).alias("received_at"),
            lit(etl_ts).alias("etl_load_date"),
            col("date_key"),
        )
    )


def build_proximity(events_df, ports_df, etl_ts):
    """Cross join current ports with events and compute Haversine distance."""
    current_ports = ports_df.filter(col("is_current") == True).select(
        col("port_key"),
        col("latitude").alias("port_lat"),
        col("longitude").alias("port_lon"),
    )

    events = events_df.select(
        col("seismic_sk"),
        col("date_key"),
        col("latitude").alias("event_lat"),
        col("longitude").alias("event_lon"),
    )

    cross = current_ports.crossJoin(events)
    with_distance = cross.withColumn(
        "distance_km",
        haversine_km(
            col("port_lat"), col("port_lon"),
            col("event_lat"), col("event_lon"),
        ).cast(DecimalType(8, 2)),
    )

    w = Window.orderBy("port_key", "seismic_sk")
    return (
        with_distance
        .withColumn("port_seismic_sk", row_number().over(w).cast(IntegerType()))
        .withColumn("within_50km", col("distance_km") <= 50)
        .withColumn("within_100km", col("distance_km") <= 100)
        .withColumn("etl_load_date", lit(etl_ts))
        .select(
            "port_seismic_sk",
            "seismic_sk",
            "port_key",
            "distance_km",
            "within_50km",
            "within_100km",
            "etl_load_date",
            "date_key",
        )
    )


def build_risk_snapshot(proximity_df, snapshot_date_key, etl_ts):
    """
    One row per port: nearest earthquake and risk classification.
    - dangerous:   nearest event <= 50 km
    - cautionary:  nearest event > 50 km and <= 100 km
    - safe:        nearest event > 100 km
    """
    w = Window.partitionBy("port_key").orderBy(col("distance_km").asc())
    nearest = (
        proximity_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    w2 = Window.orderBy("port_key")
    return (
        nearest
        .withColumn("port_risk_sk", row_number().over(w2).cast(IntegerType()))
        .withColumn(
            "classification",
            when(col("distance_km") <= 50, lit("dangerous"))
            .when(col("distance_km") <= 100, lit("cautionary"))
            .otherwise(lit("safe")),
        )
        .withColumn("nearest_event_key", col("seismic_sk"))
        .withColumn("nearest_distance_km", col("distance_km"))
        .withColumn("date_key", lit(snapshot_date_key))
        .withColumn("etl_load_date", lit(etl_ts))
        .select(
            "port_risk_sk",
            "port_key",
            "nearest_event_key",
            "nearest_distance_km",
            "classification",
            "etl_load_date",
            "date_key",
        )
    )


try:
    spark = (
        SparkSession.builder
        .appName("BronzeToGoldEarthquakes")
        .config("spark.sql.shuffle.partitions", "8")
        .enableHiveSupport()
        .getOrCreate()
    )
    logger.info("Spark session created successfully")

    etl_ts = datetime.now()
    snapshot_date_key = int(etl_ts.strftime("%Y%m%d"))

    bronze_df = read_bronze_earthquakes(spark)
    bronze_count = bronze_df.count()
    logger.info("Bronze earthquakes loaded: %d rows", bronze_count)

    if bronze_count == 0:
        raise RuntimeError(f"No earthquake data found at {BRONZE_PATH}")

    fact_seismic = build_fact_seismic_event(bronze_df, etl_ts)
    event_count = fact_seismic.count()
    logger.info("Fact_Seismic_Event rows: %d", event_count)

    (
        fact_seismic.write
        .mode("overwrite")
        .partitionBy("date_key")
        .parquet(GOLD_FACT_SEISMIC_PATH)
    )

    spark.sql("MSCK REPAIR TABLE tidaline_gold.Fact_Seismic_Event")

    logger.info("Fact_Seismic_Event written to %s", GOLD_FACT_SEISMIC_PATH)

    ports_df = spark.read.parquet(GOLD_DIM_PORT_PATH)
    port_count = ports_df.filter(col("is_current") == True).count()
    logger.info("Current Dim_Port rows for proximity: %d", port_count)

    if port_count == 0:
        raise RuntimeError(
            "Dim_Port has no current rows. Run gold_ports_job.py first."
        )

    proximity = build_proximity(fact_seismic, ports_df, etl_ts)
    prox_count = proximity.count()
    logger.info("Fact_Port_Seismic_Proximity rows: %d", prox_count)

    (
        proximity.write
        .mode("overwrite")
        .partitionBy("date_key")
        .parquet(GOLD_PROXIMITY_PATH)
    )

    spark.sql("MSCK REPAIR TABLE tidaline_gold.Fact_Port_Seismic_Proximity")

    logger.info("Proximity fact written to %s", GOLD_PROXIMITY_PATH)

    risk_snapshot = build_risk_snapshot(proximity, snapshot_date_key, etl_ts)
    risk_count = risk_snapshot.count()
    logger.info("Fact_Port_Risk_Snapshot rows: %d", risk_count)

    (
        risk_snapshot.write
        .mode("overwrite")
        .partitionBy("date_key")
        .parquet(GOLD_RISK_SNAPSHOT_PATH)
    )

    spark.sql("MSCK REPAIR TABLE tidaline_gold.Fact_Port_Risk_Snapshot")

    logger.info("Risk snapshot written to %s", GOLD_RISK_SNAPSHOT_PATH)

except Exception as exc:
    logger.error("Gold Earthquakes ETL failed: %s", str(exc))
    raise
finally:
    if "spark" in dir():
        spark.stop()
    logger.info("Gold Earthquakes ETL process completed")
