from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "earthquicks-cdc.public.earthquakes"
CHECKPOINT_PATH = "/checkpoints/kafka_to_snowflake"

SNOWFLAKE_OPTIONS = {
    "sfURL": "AZZKOPF-FI96094.snowflakecomputing.com",
    "sfUser": "itiGrad",
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": "MARITIME_LOGISTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "dbtable": "EARTHQUAKES_REALTIME",
}


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Kafka-To-Snowflake")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read Kafka stream
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# TODO: parse the Kafka `value` JSON into the real earthquake
# columns here, matching EARTHQUAKES_REALTIME's schema
# (this is the piece we still need DESCRIBE TABLE for)
# ============================================================

parsed_stream = (
    raw_stream
    .select(
        col("value").cast("string").alias("json_str")
    )
    # .withColumn("parsed", from_json(col("json_str"), earthquake_schema))
    # .select("parsed.*")
)


# ============================================================
# Write to Snowflake via foreachBatch
# ============================================================

def write_to_snowflake(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .mode("append")
        .save()
    )


snowflake_query = (
    parsed_stream
    .writeStream
    .foreachBatch(write_to_snowflake)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

snowflake_query.awaitTermination()