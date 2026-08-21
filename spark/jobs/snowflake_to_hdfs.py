import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

# ============================================================
# 1. Spark Session
# ============================================================
spark = SparkSession.builder.appName("SnowflakeToHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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
HDFS_PATH = "hdfs://namenode:8020/bronze/earthquakes"

# Safety margin: only archive/delete data older than 25 hours, not exactly
# 24, to make sure no rows from the streaming job are still in flight
# (late-arriving events).
SAFETY_MARGIN_HOURS = 25
watermark = datetime.utcnow() - timedelta(hours=SAFETY_MARGIN_HOURS)
watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")

print(f"Running Snowflake -> HDFS archival | watermark (UTC): {watermark_str}")


def run_snowflake_sql(query):
    jvm = spark.sparkContext._jvm
    options_map = jvm.PythonUtils.toScalaMap(sfOptions)
    jvm.net.snowflake.spark.snowflake.Utils.runQuery(options_map, query)


# ============================================================
# 4. Read from Snowflake only the rows older than the watermark
# ============================================================
select_query = f"""
    SELECT * FROM {TABLE_NAME}
    WHERE CDC_TIMESTAMP < '{watermark_str}'
"""

df = (
    spark.read.format("snowflake")
    .options(**sfOptions)
    .option("query", select_query)
    .load()
    .cache()
)

rows_to_archive = df.count()
print(f"Rows eligible for archival: {rows_to_archive}")

if rows_to_archive == 0:
    print("No data old enough to archive today. Job exiting, nothing changed.")
    spark.stop()
    sys.exit(0)

# ============================================================
# 5. Partition column is still called "date" (matches what the Hive team
#    already expects), but its value is now the actual event date
#    (EVENT_TIME), not the run date.
# ============================================================
df = df.withColumn("date", date_format(col("EVENT_TIME"), "yyyy-MM-dd"))

# ============================================================
# 6. Write to HDFS as CSV (same shape and options as before)
# ============================================================
(
    df.write
    .mode("append")
    .partitionBy("date")
    .option("header", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
    .option("dateFormat", "yyyy-MM-dd")
    .csv(HDFS_PATH)
)
print(f"Written to {HDFS_PATH} (partitioned by event date, same column name 'date')")

# ============================================================
# 7. Verify: read back from HDFS as CSV and confirm the count matches
#    before deleting anything
# ============================================================
verify_df = (
    spark.read
    .option("header", "true")
    .csv(HDFS_PATH)
    .filter(f"CDC_TIMESTAMP < '{watermark_str}'")
)
rows_in_hdfs = verify_df.count()
print(f"Rows found in HDFS after write (same watermark): {rows_in_hdfs}")

if rows_in_hdfs < rows_to_archive:
    print("=" * 70)
    print("!!! WARNING: row count in HDFS is lower than expected. Nothing will be deleted from Snowflake.")
    print(f"    Expected: {rows_to_archive} | Actually found: {rows_in_hdfs}")
    print("=" * 70)
    spark.stop()
    sys.exit(1)

# ============================================================
# 8. Delete from Snowflake -- last step, only if verification passed
# ============================================================
print("Row counts match. Deleting archived rows from Snowflake...")
delete_query = f"""
    DELETE FROM {TABLE_NAME}
    WHERE CDC_TIMESTAMP < '{watermark_str}'
"""
run_snowflake_sql(delete_query)

print(f"Deleted {rows_to_archive} rows from Snowflake successfully. Archival complete.")
spark.stop()
