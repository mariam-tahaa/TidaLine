"""
Gold Ports ETL: Silver -> Dim_Date + Dim_Port (SCD Type 2).

Reads the latest Silver_Ports partition and applies SCD2 logic to
track historical changes in port attributes over time.
"""
from datetime import datetime
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, concat_ws, current_timestamp, date_format,
    dayofmonth, explode, lit, max as spark_max, month, quarter,
    row_number, sequence, to_date, when, year,
)
from pyspark.sql.types import (
    BooleanType, DecimalType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

sys.path.insert(0, "/opt/spark-apps")
from logger import Logger


logger = Logger(log_file="/tmp/gold_ports_etl.log")
logger.info("Gold Ports ETL process started")

END_DATE_CURRENT = 99991231
DATE_DIM_START = "2020-01-01"
DATE_DIM_END = "2030-12-31"

GOLD_DIM_DATE_PATH = "hdfs://namenode:8020/gold/dim_date"
GOLD_DIM_PORT_TABLE = "tidaline_gold.Dim_Port"
GOLD_DIM_PORT_PATH = "hdfs://namenode:8020/gold/dim_port"

SILVER_TABLE = "tidaline_silver.Silver_Ports"

# Columns compared exclusively for SCD Type 2 change detection.
# If any of these values change for a port, the old row is expired and a new
# version is inserted. Identity/location fields (name, country, coordinates)
# are loaded into Dim_Port but not used to trigger new SCD2 versions.
SCD_COMPARE_COLS = [
    "harbor_size",
    "harbor_use",
    "shelter_afforded",
    "supplies_rate",
    "comm_rate",
    "channel_depth",
    "anchorage_depth",
    "cargo_pier_depth",
    "oil_terminal_depth",
    "LNG_terminal_depth",
]

DIM_PORT_SCHEMA = StructType([
    StructField("port_key", IntegerType(), False),
    StructField("world_port_index_number", IntegerType(), False),
    StructField("main_port_name", StringType(), True),
    StructField("harbor_size", StringType(), True),
    StructField("harbor_type", StringType(), True),
    StructField("harbor_use", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("latitude", DecimalType(9, 6), True),
    StructField("longitude", DecimalType(9, 6), True),
    StructField("shelter_afforded", StringType(), True),
    StructField("supplies_rate", StringType(), True),
    StructField("comm_rate", StringType(), True),
    StructField("channel_depth", DecimalType(5, 1), True),
    StructField("anchorage_depth", DecimalType(5, 1), True),
    StructField("cargo_pier_depth", DecimalType(5, 1), True),
    StructField("oil_terminal_depth", DecimalType(5, 1), True),
    StructField("LNG_terminal_depth", DecimalType(5, 1), True),
    StructField("effective_date", IntegerType(), False),
    StructField("end_date", IntegerType(), False),
    StructField("is_current", BooleanType(), False),
    StructField("etl_load_date", TimestampType(), False),
])


def build_dim_date(spark):
    """Build or refresh the date dimension."""
    dim_date = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(DATE_DIM_START)),
                    to_date(lit(DATE_DIM_END)),
                    lit(1).cast("interval day"),
                )
            ).alias("full_date")
        )
        .select(
            date_format("full_date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
            col("full_date"),
            dayofmonth("full_date").alias("day_num"),
            month("full_date").alias("month_num"),
            date_format("full_date", "MMMM").alias("month_name"),
            quarter("full_date").alias("quarter_num"),
            year("full_date").alias("year_num"),
            date_format("full_date", "EEEE").alias("day_of_week"),
        )
    )
    return dim_date


def map_silver_to_port_staging(silver_df):
    """Map Silver_Ports columns to Dim_Port attribute layout."""
    return silver_df.select(
        col("world_port_index_number").cast(IntegerType()).alias("world_port_index_number"),
        col("main_port_name"),
        col("harbor_size"),
        col("harbor_type"),
        col("harbor_use"),
        col("country_code"),
        col("region_name"),
        col("latitude").cast(DecimalType(9, 6)).alias("latitude"),
        col("longitude").cast(DecimalType(9, 6)).alias("longitude"),
        col("shelter_afforded"),
        col("supplies_rate"),
        col("comm_rate"),
        col("channel_depth_m").cast(DecimalType(5, 1)).alias("channel_depth"),
        col("anchorage_depth_m").cast(DecimalType(5, 1)).alias("anchorage_depth"),
        col("cargo_pier_depth_m").cast(DecimalType(5, 1)).alias("cargo_pier_depth"),
        col("oil_terminal_depth_m").cast(DecimalType(5, 1)).alias("oil_terminal_depth"),
        col("lng_terminal_depth_m").cast(DecimalType(5, 1)).alias("LNG_terminal_depth"),
    )


def add_change_hash(df, alias="change_hash"):
    """Build a concatenated fingerprint of SCD-tracked columns."""
    string_cols = [coalesce(col(c).cast("string"), lit("")) for c in SCD_COMPARE_COLS]
    return df.withColumn(alias, concat_ws("|", *string_cols))


def apply_scd2(spark, staging_df, existing_df, effective_date_key, etl_ts):
    """
    Apply SCD Type 2:
    - Insert new ports
    - Expire changed ports and insert new versions
    - Keep unchanged current rows as-is
    """
    if existing_df.rdd.isEmpty():
        max_key = 0
        current_df = spark.createDataFrame([], DIM_PORT_SCHEMA)
    else:
        max_key = existing_df.agg(spark_max("port_key")).collect()[0][0] or 0
        current_df = existing_df.filter(col("is_current") == True)

    staging_h = add_change_hash(staging_df, "staging_hash")
    current_h = add_change_hash(current_df, "current_hash")

    joined = staging_h.alias("s").join(
        current_h.alias("c"),
        col("s.world_port_index_number") == col("c.world_port_index_number"),
        "left",
    )

    new_ports = joined.filter(col("c.port_key").isNull()).select("s.*", col("staging_hash"))
    changed_ports = joined.filter(
        col("c.port_key").isNotNull() & (col("staging_hash") != col("current_hash"))
    ).select("s.*", col("c.port_key").alias("old_port_key"), col("staging_hash"))
    unchanged_keys = joined.filter(
        col("c.port_key").isNotNull() & (col("staging_hash") == col("current_hash"))
    ).select(col("c.port_key"))

    unchanged_df = current_df.join(unchanged_keys, "port_key", "inner")

    expired_df = (
        current_df.alias("cur")
        .join(changed_ports.select("old_port_key").distinct(), col("cur.port_key") == col("old_port_key"))
        .select(
            col("cur.port_key"),
            col("cur.world_port_index_number"),
            col("cur.main_port_name"),
            col("cur.harbor_size"),
            col("cur.harbor_type"),
            col("cur.harbor_use"),
            col("cur.country_code"),
            col("cur.region_name"),
            col("cur.latitude"),
            col("cur.longitude"),
            col("cur.shelter_afforded"),
            col("cur.supplies_rate"),
            col("cur.comm_rate"),
            col("cur.channel_depth"),
            col("cur.anchorage_depth"),
            col("cur.cargo_pier_depth"),
            col("cur.oil_terminal_depth"),
            col("cur.LNG_terminal_depth"),
            col("cur.effective_date"),
            lit(effective_date_key).alias("end_date"),
            lit(False).alias("is_current"),
            col("cur.etl_load_date"),
        )
    )

    historical_df = existing_df.filter(col("is_current") == False)
    if not expired_df.rdd.isEmpty():
        historical_df = historical_df.unionByName(expired_df)

    inserts = []
    next_key = max_key

    if not new_ports.rdd.isEmpty():
        w = Window.orderBy("world_port_index_number")
        new_dim = (
            new_ports
            .withColumn("port_key", row_number().over(w) + lit(next_key))
            .select(
                col("port_key").cast(IntegerType()),
                col("world_port_index_number"),
                col("main_port_name"),
                col("harbor_size"),
                col("harbor_type"),
                col("harbor_use"),
                col("country_code"),
                col("region_name"),
                col("latitude"),
                col("longitude"),
                col("shelter_afforded"),
                col("supplies_rate"),
                col("comm_rate"),
                col("channel_depth"),
                col("anchorage_depth"),
                col("cargo_pier_depth"),
                col("oil_terminal_depth"),
                col("LNG_terminal_depth"),
                lit(effective_date_key).alias("effective_date"),
                lit(END_DATE_CURRENT).alias("end_date"),
                lit(True).alias("is_current"),
                lit(etl_ts).alias("etl_load_date"),
            )
        )
        inserts.append(new_dim)
        next_key += new_ports.count()

    if not changed_ports.rdd.isEmpty():
        w = Window.orderBy("world_port_index_number")
        changed_dim = (
            changed_ports
            .withColumn("port_key", row_number().over(w) + lit(next_key))
            .select(
                col("port_key").cast(IntegerType()),
                col("world_port_index_number"),
                col("main_port_name"),
                col("harbor_size"),
                col("harbor_type"),
                col("harbor_use"),
                col("country_code"),
                col("region_name"),
                col("latitude"),
                col("longitude"),
                col("shelter_afforded"),
                col("supplies_rate"),
                col("comm_rate"),
                col("channel_depth"),
                col("anchorage_depth"),
                col("cargo_pier_depth"),
                col("oil_terminal_depth"),
                col("LNG_terminal_depth"),
                lit(effective_date_key).alias("effective_date"),
                lit(END_DATE_CURRENT).alias("end_date"),
                lit(True).alias("is_current"),
                lit(etl_ts).alias("etl_load_date"),
            )
        )
        inserts.append(changed_dim)

    result = historical_df
    if not unchanged_df.rdd.isEmpty():
        result = result.unionByName(unchanged_df)
    for part in inserts:
        result = result.unionByName(part)

    return result


try:
    spark = (
        SparkSession.builder
        .appName("SilverToGoldPorts")
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate()
    )
    logger.info("Spark session created successfully")

    etl_ts = datetime.now()
    effective_date_key = int(etl_ts.strftime("%Y%m%d"))

    # -------------------------------------------------------------------------
    # Dim_Date
    # -------------------------------------------------------------------------
    dim_date = build_dim_date(spark)
    dim_date.write.mode("overwrite").parquet(GOLD_DIM_DATE_PATH)
    logger.info("Dim_Date written: %d rows", dim_date.count())

    # -------------------------------------------------------------------------
    # Read latest Silver partition
    # -------------------------------------------------------------------------
    latest_load_date = (
        spark.sql(f"SELECT MAX(load_date) AS d FROM {SILVER_TABLE}")
        .collect()[0]["d"]
    )
    logger.info("Using Silver load_date=%s", latest_load_date)

    silver_df = spark.sql(
        f"SELECT * FROM {SILVER_TABLE} WHERE load_date = '{latest_load_date}'"
    )
    staging_df = map_silver_to_port_staging(silver_df)
    logger.info("Silver staging rows: %d", staging_df.count())

    # -------------------------------------------------------------------------
    # Dim_Port SCD2
    # -------------------------------------------------------------------------
    try:
        existing_dim = spark.read.parquet(GOLD_DIM_PORT_TABLE)
        logger.info("Existing Dim_Port rows: %d", existing_dim.count())
    except Exception:
        existing_dim = spark.createDataFrame([], DIM_PORT_SCHEMA)
        logger.info("No existing Dim_Port found; initial load")

    dim_port = apply_scd2(spark, staging_df, existing_dim, effective_date_key, etl_ts)
    dim_port.write.mode("overwrite").parquet(GOLD_DIM_PORT_PATH)

    current_count = dim_port.filter(col("is_current") == True).count()
    total_count = dim_port.count()
    logger.info("Dim_Port written: %d total, %d current", total_count, current_count)

except Exception as exc:
    logger.error("Gold Ports ETL failed: %s", str(exc))
    raise
finally:
    if "spark" in dir():
        spark.stop()
    logger.info("Gold Ports ETL process completed")
