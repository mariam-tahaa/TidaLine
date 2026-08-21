### Set up the Snowflake password as an Airflow Variable (UI)

Before the DAG can connect to Snowflake, the `snowflake_password` Variable must exist — the DAG references it as `{{ var.value.snowflake_password }}` and the Spark job reads it from the `SNOWFLAKE_PASSWORD` environment variable.

1. Open the Airflow web UI (default: `http://localhost:8080`).
2. Go to **Admin → Variables** in the top navigation bar.
3. Click the **+** (blue plus) button to add a new record.
4. Fill in:
   - **Key:** `snowflake_password`
   - **Val:** your actual Snowflake password
   - **Description** *(optional)*: `Password for Snowflake connection used by kafka_to_snowflake DAG`
5. Click **Save**.

You should now see `snowflake_password` listed under Admin → Variables, with its value masked (`***`) in the UI for security. This keeps the password out of the DAG file and out of Git entirely — the DAG only ever references it by key.

> To update the password later, click the row's edit (pencil) icon under Admin → Variables and save the new value — no DAG code changes needed.

---

### Test and trigger from the Airflow UI

1. Open the Airflow UI and locate `tidaline_seismic_realtime` in the DAGs list.
2. Toggle the DAG **On** (the switch on the left of its row) if it isn't already — this only enables it for use, it will **not** run automatically since `schedule=None`.
3. Click the **▶ Trigger DAG** button (top right of the DAG's page, or the play icon on its row).
4. Open the DAG's **Graph** view to watch tasks execute in order: `check_kafka → check_seismic_topic → check_and_clean_stream_container → kafka_to_snowflake`. Each task turns dark green when it succeeds.
5. Click on the `kafka_to_snowflake` task box, then **Logs**, to see the live Spark output (dependency resolution, Kafka connection, and each micro-batch's MERGE result).
6. This last task will stay **blue/running** indefinitely — that's expected for a streaming job, not a stuck task. To stop it, use **Clear** or manually stop the container (`docker stop airflow_kafka_to_snowflake`) as covered earlier in this README.
7. If a task fails, click it → **Logs** to see the traceback, or check **Admin → Variables** first if the failure is a Snowflake authentication error (usually means the Variable above is missing or has a stale password).
