from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Initialize Spark Session configured for Debezium CDC processing
spark = (
    SparkSession.builder.appName("TidaLineDebeziumConsumer")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

# Set logging level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define schema matching Debezium CDC payload structure
after_schema = StructType(
    [
        StructField("unid", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_catalog", StringType(), True),
        StructField("lastupdate", LongType(), True),
        StructField("time", LongType(), True),
        StructField("flynn_region", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("evtype", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("mag", DoubleType(), True),
        StructField("magtype", StringType(), True),
        StructField("action", StringType(), True),
        StructField("received_at", LongType(), True),
    ]
)

payload_schema = StructType(
    [
        StructField("before", after_schema, True),
        StructField("after", after_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ]
)

root_schema = StructType(
    [
        StructField("payload", payload_schema, True),
    ]
)

# Read streaming data from Kafka internal listener port
raw_kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "earthquicks-cdc.public.earthquakes")
    .option("startingOffsets", "earliest")
    .load()
)

# Deserialize JSON payload and extract nested Debezium fields
parsed_df = (
    raw_kafka_df
    .selectExpr("CAST(value AS STRING) AS json_payload")
    .select(
        from_json(
            col("json_payload"),
            root_schema
        ).alias("data")
    )
    .select(
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("cdc_timestamp"),
        col("data.payload.after.*")
    )
)

# Output parsed stream directly to console
query = (
    parsed_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()