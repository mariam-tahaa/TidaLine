from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
from airflow.operators.bash import BashOperator

# ============================================================
# Configuration
# ============================================================

NETWORK = "tidaline-case-study-network"

SPARK_IMAGE = "apache/spark:3.5.3"

KAFKA_IMAGE = "confluentinc/cp-kafka:7.4.0"

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

SEISMIC_TOPIC = "earthquicks-cdc.public.earthquakes"


# ============================================================
# Spark / Hadoop configuration
# ============================================================

SPARK_MOUNTS = [

    Mount(
        source=(
            "D:/ITI/graduation_project/"
            "TidaLine/hadoop/conf/core-site.xml"
        ),
        target="/opt/spark/conf/core-site.xml",
        type="bind",
        read_only=True,
    ),

    Mount(
        source=(
            "D:/ITI/graduation_project/"
            "TidaLine/hadoop/conf/hdfs-site.xml"
        ),
        target="/opt/spark/conf/hdfs-site.xml",
        type="bind",
        read_only=True,
    ),

    Mount(
        source=(
            "D:/ITI/graduation_project/"
            "TidaLine/hive/conf/hive-site.xml"
        ),
        target="/opt/spark/conf/hive-site.xml",
        type="bind",
        read_only=True,
    ),

    Mount(
        source=(
            "D:/ITI/graduation_project/"
            "TidaLine/spark/jobs"
        ),
        target="/opt/spark-apps",
        type="bind",
        read_only=True,
    ),

    Mount(
        source=(
            "D:/ITI/graduation_project/"
            "TidaLine/utils/logger.py"
        ),
        target="/opt/spark-apps/logger.py",
        type="bind",
        read_only=True,
    ),

]


# ============================================================
# DAG
#
# This DAG is manually triggered.
#
# It starts ONE long-running Spark Structured Streaming
# application.
#
# PostgreSQL
#     ↓
# Debezium
#     ↓
# Kafka
#     ↓
# Spark
#     ↓
# HDFS Bronze
# ============================================================

with DAG(

    dag_id="tidaline_seismic_realtime",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "tidaline",
        "seismic",
        "realtime",
        "kafka",
        "debezium",
        "spark",
    ],

) as dag:


    # ========================================================
    # Check Kafka
    # ========================================================

    check_kafka = DockerOperator(

        task_id="check_kafka",

        image=KAFKA_IMAGE,

        container_name="airflow_check_kafka_seismic",

        api_version="auto",

        auto_remove=True,

        command=(
            "bash -c "
            "\""
            "set -e && "
            "echo 'Checking Kafka...' && "
            "kafka-topics "
            "--bootstrap-server kafka:29092 "
            "--list && "
            "echo 'Kafka is reachable'"
            "\""
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode=NETWORK,

        mount_tmp_dir=False,

    )


    # ========================================================
    # Check seismic topic
    # ========================================================

    check_seismic_topic = DockerOperator(

        task_id="check_seismic_topic",

        image=KAFKA_IMAGE,

        container_name="airflow_check_seismic_topic",

        api_version="auto",

        auto_remove=True,

        command=(
            "bash -c "
            "\""
            "set -e && "
            "echo 'Checking seismic topic...' && "
            "kafka-topics "
            "--bootstrap-server kafka:29092 "
            "--describe "
            "--topic "
            "earthquicks-cdc.public.earthquakes && "
            "echo 'Seismic topic exists'"
            "\""
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode=NETWORK,

        mount_tmp_dir=False,

    )

    check_and_clean = BashOperator(
        task_id="check_and_clean_stream_container",
        bash_command=(
            "if docker inspect airflow_seismic_stream >/dev/null 2>&1; then "
            "  RUNNING=$(docker inspect -f {{ '{{.State.Running}}' }} airflow_seismic_stream); "
            "  if [ \"$RUNNING\" = \"true\" ]; then "
            "    echo 'Stream already running, skipping restart' && exit 1; "
            "  else "
            "    echo 'Removing stale container' && docker rm -f airflow_seismic_stream; "
            "  fi; "
            "else "
            "  echo 'No existing container found'; "
            "fi"
        ),
    )


    # ========================================================
    # Spark Structured Streaming
    # ========================================================

    seismic_stream = DockerOperator(

        task_id="seismic_stream",

        image=SPARK_IMAGE,

        container_name="airflow_seismic_stream",

        api_version="auto",

        # IMPORTANT:
        #
        # The Spark streaming application never finishes
        # because it uses awaitTermination().
        #
        # Therefore we do NOT use auto_remove=True.
        #

        auto_remove=False,

        user="root",

        command=(

            "/opt/spark/bin/spark-submit "

            "--master local[*] "

            "--packages "
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 "

            "--conf "
            "spark.sql.streaming.checkpointLocation="
            "/opt/checkpoints/seismic "

            "/opt/spark-apps/seismic_stream.py"

        ),

        docker_url="unix://var/run/docker.sock",

        network_mode=NETWORK,

        mount_tmp_dir=False,

        mounts=SPARK_MOUNTS + [

            Mount(

                source=(
                    "D:/ITI/graduation_project/"
                    "TidaLine/hadoop/checkpoints"
                ),

                target="/opt/checkpoints",

                type="bind",

            ),

        ],

    )


    # ========================================================
    # Dependency
    # ========================================================

    (
        check_kafka
        >> check_seismic_topic
        >> check_and_clean
        >> seismic_stream
    )