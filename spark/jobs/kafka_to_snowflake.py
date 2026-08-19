from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    expr,
    row_number,
    coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructType,
    StructField
)


# =============================================================================
# 1. Spark Session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("KafkaToSnowflakeStream")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)


# =============================================================================
# 2. Snowflake Configuration
# =============================================================================

sfOptions = {
    "sfURL": "ct21783.eu-central-2.aws.snowflakecomputing.com",
    "sfUser": "itiGrad",
    "sfPassword": "s92te_QMm2M_r5J",
    "sfDatabase": "MARITIME_LOGISTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
}


# =============================================================================
# 3. Snowflake Tables
# =============================================================================

STAGING_TABLE = "EARTHQUAKES_STAGING"
TARGET_TABLE = "EARTHQUAKES_REALTIME"


# =============================================================================
# 4. Source Row Schema
# =============================================================================


row_schema = StructType([
    StructField("unid", StringType()),
    StructField("source_id", StringType()),
    StructField("source_catalog", StringType()),
    StructField("lastupdate", LongType()),
    StructField("time", LongType()),
    StructField("flynn_region", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("depth", DoubleType()),
    StructField("evtype", StringType()),
    StructField("auth", StringType()),
    StructField("mag", DoubleType()),
    StructField("magtype", StringType()),
    StructField("action", StringType()),
    StructField("received_at", LongType()),
])


# =============================================================================
# 5. Debezium Payload Schema
# =============================================================================
#
# op:
#   c = create
#   u = update
#   d = delete
#   r = read/snapshot
#
# ts_ms = CDC timestamp in milliseconds
#

payload_schema = StructType([
    StructField("before", row_schema),
    StructField("after", row_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])


# =============================================================================
# 6. Debezium Envelope Schema
# =============================================================================

envelope_schema = StructType([
    StructField("payload", payload_schema),
])


# =============================================================================
# 7. Read Kafka Stream
# =============================================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "earthquicks-cdc.public.earthquakes")
    .load()
)


# =============================================================================
# 8. Parse Kafka JSON
# =============================================================================

parsed_df = (
    kafka_df

    # Kafka value -> String
    .selectExpr("CAST(value AS STRING) as json_str")

    # JSON -> Struct
    .select(
        from_json(
            col("json_str"),
            envelope_schema
        ).alias("data")
    )

    # Extract Debezium payload
    .select("data.payload.*")

    # For DELETE:
    # after = null
    # before contains the old record
    #
    # For CREATE / UPDATE:
    # after contains the current record
    .withColumn(
        "record",
        coalesce(
            col("after"),
            col("before")
        )
    )

    # Select required columns
    .select(
        col("op").alias("operation"),
        col("ts_ms").alias("cdc_timestamp"),

        col("record.unid").alias("unid"),
        col("record.source_id").alias("source_id"),
        col("record.source_catalog").alias("source_catalog"),

        col("record.lastupdate").alias("lastupdate"),
        col("record.time").alias("time"),

        col("record.flynn_region").alias("flynn_region"),
        col("record.lat").alias("lat"),
        col("record.lon").alias("lon"),
        col("record.depth").alias("depth"),

        col("record.evtype").alias("evtype"),
        col("record.auth").alias("auth"),

        col("record.mag").alias("mag"),
        col("record.magtype").alias("magtype"),

        col("record.action").alias("action"),
        col("record.received_at").alias("received_at"),
    )

    # Ignore records without UNID
    .filter(
        col("unid").isNotNull()
    )

    # CDC timestamp:
    # milliseconds -> timestamp
    .withColumn(
        "cdc_timestamp",
        expr("timestamp_millis(cdc_timestamp)")
    )

    # lastupdate:
    # microseconds -> timestamp
    .withColumn(
        "lastupdate",
        expr("timestamp_micros(lastupdate)")
    )

    # event time:
    # microseconds -> timestamp
    .withColumn(
        "event_time",
        expr("timestamp_micros(time)")
    )

    # received_at:
    # microseconds -> timestamp
    .withColumn(
        "received_at",
        expr("timestamp_micros(received_at)")
    )

    # time is no longer needed
    .drop("time")
)


# =============================================================================
# 9. Execute SQL on Snowflake
# =============================================================================
#
# Used for:
#   - TRUNCATE
#   - MERGE
#
# through the Snowflake Spark Connector JVM utilities.
#

def run_snowflake_sql(spark, sfOptions, query):

    jvm = spark.sparkContext._jvm

    options_map = (
        jvm.PythonUtils.toScalaMap(sfOptions)
    )

    jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        options_map,
        query
    )


# =============================================================================
# 10. Write Each Micro-Batch to Snowflake
# =============================================================================

def write_to_snowflake(batch_df, batch_id):

    # -------------------------------------------------------------------------
    # Skip empty micro-batches
    # -------------------------------------------------------------------------

    if batch_df.rdd.isEmpty():
        return


    # -------------------------------------------------------------------------
    # Deduplication inside the current micro-batch
    #
    # Same UNID may appear multiple times:
    #
    #     create
    #     update
    #
    # We keep the latest event based on CDC timestamp.
    # -------------------------------------------------------------------------

    window_spec = (
        Window
        .partitionBy("unid")
        .orderBy(
            col("cdc_timestamp").desc()
        )
    )

    deduped_df = (
        batch_df

        .withColumn(
            "rn",
            row_number().over(window_spec)
        )

        .filter(
            col("rn") == 1
        )

        .drop("rn")
    )


    # -------------------------------------------------------------------------
    # Clear staging table
    #
    # IMPORTANT:
    #
    # We do NOT use:
    #
    #     mode("overwrite")
    #
    # together with:
    #
    #     column_mapping = name
    #
    # because Snowflake Connector supports column mapping only with append.
    #
    # Therefore:
    #
    #     TRUNCATE
    #         +
    #     APPEND
    #
    # -------------------------------------------------------------------------

    run_snowflake_sql(
        spark,
        sfOptions,
        f"TRUNCATE TABLE {STAGING_TABLE}"
    )


    # -------------------------------------------------------------------------
    # Write current micro-batch to staging
    # -------------------------------------------------------------------------

    (
        deduped_df.write
        .format("snowflake")
        .options(**sfOptions)

        .option(
            "dbtable",
            STAGING_TABLE
        )

        # Match DataFrame columns with Snowflake columns by name
        .option(
            "column_mapping",
            "name"
        )

        # Fail if columns don't match
        .option(
            "column_mismatch_behavior",
            "error"
        )

        # IMPORTANT:
        # column_mapping=name requires append mode
        .mode("append")

        .save()
    )


    # -------------------------------------------------------------------------
    # MERGE staging -> target
    # -------------------------------------------------------------------------
    #
    # Match records using UNID.
    #
    # If record exists:
    #     update only when staging CDC_TIMESTAMP is newer.
    #
    # If record doesn't exist:
    #     insert it.
    #
    # -------------------------------------------------------------------------

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS target

        USING {STAGING_TABLE} AS staging

        ON target.UNID = staging.UNID


        WHEN MATCHED
             AND staging.CDC_TIMESTAMP > target.CDC_TIMESTAMP

        THEN UPDATE SET

            target.OPERATION      = staging.OPERATION,
            target.CDC_TIMESTAMP  = staging.CDC_TIMESTAMP,
            target.SOURCE_ID      = staging.SOURCE_ID,
            target.SOURCE_CATALOG = staging.SOURCE_CATALOG,

            target.LASTUPDATE     = staging.LASTUPDATE,
            target.EVENT_TIME     = staging.EVENT_TIME,

            target.FLYNN_REGION   = staging.FLYNN_REGION,

            target.LAT            = staging.LAT,
            target.LON            = staging.LON,
            target.DEPTH          = staging.DEPTH,

            target.EVTYPE         = staging.EVTYPE,
            target.AUTH           = staging.AUTH,

            target.MAG            = staging.MAG,
            target.MAGTYPE        = staging.MAGTYPE,

            target.ACTION         = staging.ACTION,
            target.RECEIVED_AT    = staging.RECEIVED_AT


        WHEN NOT MATCHED

        THEN INSERT (

            OPERATION,
            CDC_TIMESTAMP,
            UNID,
            SOURCE_ID,
            SOURCE_CATALOG,

            LASTUPDATE,
            EVENT_TIME,

            FLYNN_REGION,

            LAT,
            LON,
            DEPTH,

            EVTYPE,
            AUTH,

            MAG,
            MAGTYPE,

            ACTION,
            RECEIVED_AT
        )

        VALUES (

            staging.OPERATION,
            staging.CDC_TIMESTAMP,
            staging.UNID,
            staging.SOURCE_ID,
            staging.SOURCE_CATALOG,

            staging.LASTUPDATE,
            staging.EVENT_TIME,

            staging.FLYNN_REGION,

            staging.LAT,
            staging.LON,
            staging.DEPTH,

            staging.EVTYPE,
            staging.AUTH,

            staging.MAG,
            staging.MAGTYPE,

            staging.ACTION,
            staging.RECEIVED_AT
        )
    """


    # Execute MERGE
    run_snowflake_sql(
        spark,
        sfOptions,
        merge_sql
    )


# =============================================================================
# 11. Start Streaming Query
# =============================================================================

parsed_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .option(
        "checkpointLocation",
        "file:///opt/spark-apps/checkpoints/kafka_to_snowflake"
    ) \
    .start() \
    .awaitTermination()
