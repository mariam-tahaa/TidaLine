from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime


# ============================================================
# DAG 1: Monthly batch ingestion (ports data → bronze → silver)
# ============================================================

with DAG(
    dag_id="tidaline_ingest_bronze_monthly",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
) as dag_ingest:

    ingest_bronze = DockerOperator(
        task_id="ingest_to_bronze",
        image="ysfetman/bdss-hadoop-base:3.4.2",
        container_name="ingest_bronze_task",
        api_version="auto",
        auto_remove=True,
        command=(
            'bash -c "'
            'hdfs dfs -mkdir -p /bronze/ports/month={{ ds_nodash[:6] }} && '
            'for f in /data/ports/*; do '
            'fname=$(basename $f); '
            'if hdfs dfs -test -e /bronze/ports/month={{ ds_nodash[:6] }}/$fname; then '
            'echo \\"Skipping $fname, already exists\\"; '
            'else '
            'hdfs dfs -put $f /bronze/ports/month={{ ds_nodash[:6] }}/; '
            'fi; '
            'done'
            '"'
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        environment={
            "HADOOP_HOME": "/opt/hadoop",
            "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
            "PATH": "/opt/hadoop/bin:/opt/hadoop/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        },
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/hadoop/etc/hadoop/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/hadoop/etc/hadoop/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/ingestion/batch/data",
                  target="/data", type="bind", read_only=True),
        ],
    )

    ingest_bronze