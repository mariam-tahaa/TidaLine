from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime


# ============================================================
# DAG 2: Daily Snowflake → hdfs export
# ============================================================

with DAG(
    dag_id="tidaline_snowflake_to_hdfs_daily",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag_snowflake:

    snowflake_to_hdfs = DockerOperator(
        task_id="snowflake_to_hdfs",
        image="apache/spark:3.5.3",
        container_name="snowflake_to_hdfs_task",
        api_version="auto",
        auto_remove=True,
        user="root",
        command=(
            "/opt/spark/bin/spark-submit "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages net.snowflake:spark-snowflake_2.12:3.1.3,net.snowflake:snowflake-jdbc:3.28.0 "
            "/opt/spark-apps/snowflake_to_hdfs.py"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/spark/jobs",
                  target="/opt/spark-apps", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/spark/jars",
                  target="/opt/spark-jars", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/spark/ivy-cache",
                  target="/tmp/.ivy2", type="bind", read_only=False),  # <-- add this
            ],
    )

    snowflake_to_hdfs