from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

TOPIC_NAME = "earthquicks-cdc.public.earthquakes"

BRONZE_PATH = "/bronze/seismic"

CHECKPOINT_PATH = "/checkpoints/seismic"

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("TidaLine-Seismic-Realtime")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read Kafka stream
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option(
        "subscribe",
        TOPIC_NAME,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .option(
        "failOnDataLoss",
        "false",
    )
    .load()
)


# ============================================================
# Convert Kafka message
# ============================================================

bronze_stream = (
    raw_stream
    .select(
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
    )
)


# ============================================================
# Write Bronze
# ============================================================

query = (
    bronze_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        BRONZE_PATH,
    )
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)


# ============================================================
# Keep streaming application alive
# ============================================================

query.awaitTermination()