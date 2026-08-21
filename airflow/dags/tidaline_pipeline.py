from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime


HADOOP_CONF_MOUNTS = [
    Mount(source="tidaline_data_ports", target="/data", type="volume"),  # adjust if using bind mount instead
]

with DAG(
    dag_id="tidaline_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:


    ingest_bronze = DockerOperator(
        task_id="ingest_to_bronze",
        image="ysfetman/bdss-hadoop-base:3.4.2",
        container_name="ingest_bronze_task",
        api_version="auto",
        auto_remove=True,
        command='bash -c "hdfs dfs -mkdir -p /data/ports && hdfs dfs -put -f /data/ports/* /data/ports/"',
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

    kafka_to_snowflake = DockerOperator(
        task_id="kafka_to_snowflake",
        image="apache/spark:3.5.3",
        container_name="kafka_to_snowflake_task",
        api_version="auto",
        auto_remove=True,
        command="/opt/spark/bin/spark-submit --master spark://spark:7077 /opt/spark-apps/kafka_to_snowflake.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
    )

    snowflake_to_bronze = DockerOperator(
        task_id="snowflake_to_bronze",
        image="apache/spark:3.5.3",
        container_name="snowflake_to_bronze_task",
        api_version="auto",
        auto_remove=True,
        command="/opt/spark/bin/spark-submit --master spark://spark:7077 /opt/spark-apps/snowflake_to_bronze.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
    )

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image="apache/spark:3.5.3",
        container_name="bronze_to_silver_task",
        api_version="auto",
        auto_remove=True,
        command="/opt/spark/bin/spark-submit --master spark://spark:7077 /opt/spark-apps/bronze_to_silver.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
    )

    silver_to_gold = DockerOperator(
        task_id="silver_to_gold",
        image="apache/spark:3.5.3",
        container_name="silver_to_gold_task",
        api_version="auto",
        auto_remove=True,
        command="/opt/spark/bin/spark-submit --master spark://spark:7077 /opt/spark-apps/silver_to_gold.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
    )

    ingest_bronze >> bronze_to_silver
    kafka_to_snowflake >> snowflake_to_bronze >> bronze_to_silver
    bronze_to_silver >> silver_to_gold