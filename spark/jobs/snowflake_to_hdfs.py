from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("SnowflakeToHDFS")
    .getOrCreate()
)


# ============================================================
# 2. Snowflake Configuration
# ============================================================

sfOptions = {
    "sfURL": "ct21783.eu-central-2.aws.snowflakecomputing.com",
    "sfUser": "itiGrad",
    "sfPassword": "s92te_QMm2M_r5J",
    "sfDatabase": "MARITIME_LOGISTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
}


# ============================================================
# 3. Configuration
# ============================================================

TABLE_NAME = "EARTHQUAKES_REALTIME"

HDFS_PATH = (
    "hdfs://namenode:8020/data/earthquakes"
)


# ============================================================
# 4. Get today's date
# ============================================================

today = date.today().isoformat()

print(f"Running Snowflake → HDFS for date: {today}")


# ============================================================
# 5. Read from Snowflake
# ============================================================

df = (
    spark.read
    .format("snowflake")
    .options(**sfOptions)
    .option("dbtable", TABLE_NAME)
    .load()
)


# ============================================================
# 6. Add partition column
# ============================================================

df = df.withColumn(
    "date",
    lit(today)
)


# ============================================================
# 7. Write to HDFS as CSV (with formatted timestamps)
# ============================================================

(
    df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .option("header", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
    .option("dateFormat", "yyyy-MM-dd")
    .csv(HDFS_PATH)
)


# ============================================================
# 8. Finish
# ============================================================

print(
    f"Successfully exported {TABLE_NAME} "
    f"to {HDFS_PATH}/date={today}/"
)

spark.stop()
