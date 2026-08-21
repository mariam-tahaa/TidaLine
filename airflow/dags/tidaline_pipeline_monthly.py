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

    hdfs_prep_silver = DockerOperator(
        task_id="hdfs_prep_silver",
        image="ysfetman/bdss-hadoop-base:3.4.2",
        container_name="hdfs_prep_silver_task",
        api_version="auto",
        auto_remove=True,
        command=(
            'bash -c "'
            'hdfs dfs -mkdir -p /user/hive/warehouse && '
            'hdfs dfs -chmod -R 777 /user/hive/warehouse && '
            'hdfs dfs -mkdir -p /silver/ports && '
            'hdfs dfs -chmod -R 777 /silver'
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
        ],
    )

    create_silver_tables = DockerOperator(
        task_id="create_silver_tables",
        image="apache/spark:3.5.3",
        container_name="create_silver_tables_task",
        api_version="auto",
        auto_remove=True,
        user="root",
        command="/opt/spark/bin/spark-sql -f /tmp/tables.sql",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/silver/tables.sql",
                  target="/tmp/tables.sql", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
        ],
    )

    repair_silver_ports = DockerOperator(
        task_id="repair_silver_ports",
        image="apache/spark:3.5.3",
        api_version="auto",
        auto_remove=True,
        user="root",
        command='/opt/spark/bin/spark-sql -e "MSCK REPAIR TABLE tidaline_silver.Silver_Ports"',
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
        ],
    )

    silver_job = DockerOperator(
        task_id="silver_job",
        image="apache/spark:3.5.3",
        container_name="silver_job_task",
        api_version="auto",
        auto_remove=True,
        user="root",
        command="/opt/spark/bin/spark-submit /opt/spark-apps/silver_job.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/medallion/silver/spark/silver_job.py",
                  target="/opt/spark-apps/silver_job.py", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/utils/logger.py",
                  target="/opt/spark-apps/logger.py", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
        ],
    )

    hdfs_prep_gold = DockerOperator(
        task_id="hdfs_prep_gold",
        image="ysfetman/bdss-hadoop-base:3.4.2",
        container_name="hdfs_prep_gold_task",
        api_version="auto",
        auto_remove=True,
        command=(
            'bash -c "'
            'hdfs dfs -mkdir -p /gold/dim_date /gold/dim_port /gold/fact_seismic_event '
            '/gold/fact_port_seismic_proximity /gold/fact_port_risk_snapshot && '
            'hdfs dfs -chmod -R 777 /gold'
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
        ],
      )

    create_gold_tables = DockerOperator(
        task_id="create_gold_tables",
        image="apache/spark:3.5.3",
        container_name="create_gold_tables_task",
        api_version="auto",
        auto_remove=True,
        user="root",
        command="/opt/spark/bin/spark-sql -f /tmp/gold_tables.sql",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/gold/tables.sql",
                  target="/tmp/gold_tables.sql", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
        ],
    )

    gold_ports_job = DockerOperator(
        task_id="gold_ports_job",
        image="apache/spark:3.5.3",
        container_name="gold_ports_job_task",
        api_version="auto",
        auto_remove=True,
        user="root",
        command="/opt/spark/bin/spark-submit /opt/spark-apps/gold_ports_job.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/medallion/gold/spark/gold_ports_job.py",
                  target="/opt/spark-apps/gold_ports_job.py", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/utils/logger.py",
                  target="/opt/spark-apps/logger.py", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
        ],
    )

    gold_earthquakes_job = DockerOperator(
        task_id="gold_earthquakes_job",
        image="apache/spark:3.5.3",
        container_name="gold_earthquakes_job_task",
        api_version="auto",
        auto_remove=True,
        user="root",
        command="/opt/spark/bin/spark-submit /opt/spark-apps/gold_earthquakes_job.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tidaline-case-study-network",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/ITI/graduation_project/TidaLine/medallion/gold/spark/gold_earthquakes_job.py",
                  target="/opt/spark-apps/gold_earthquakes_job.py", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/utils/logger.py",
                  target="/opt/spark-apps/logger.py", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/core-site.xml",
                  target="/opt/spark/conf/core-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hadoop/conf/hdfs-site.xml",
                  target="/opt/spark/conf/hdfs-site.xml", type="bind", read_only=True),
            Mount(source="D:/ITI/graduation_project/TidaLine/hive/conf/hive-site.xml",
                  target="/opt/spark/conf/hive-site.xml", type="bind", read_only=True),
        ],
    )

    ingest_bronze >> hdfs_prep_silver
    hdfs_prep_silver >> create_silver_tables 
    create_silver_tables >> repair_silver_ports 
    repair_silver_ports >> silver_job >> hdfs_prep_gold 
    hdfs_prep_gold >> create_gold_tables >> gold_ports_job 
    gold_ports_job >> gold_earthquakes_job