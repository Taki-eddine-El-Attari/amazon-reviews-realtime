from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_kafka():
    from kafka import KafkaConsumer

    consumer = None
    try:
        consumer = KafkaConsumer(bootstrap_servers="kafka:29092")
        topics = consumer.topics()
        if "amazon-reviews" not in topics:
            raise RuntimeError("Kafka topic 'amazon-reviews' introuvable")
        print("Kafka OK - topic amazon-reviews detecte")
    finally:
        if consumer:
            consumer.close()


def check_mongodb():
    from pymongo import MongoClient

    client = MongoClient(
        "mongodb://admin:admin123@mongodb:27017/",
        serverSelectionTimeoutMS=4000,
    )
    try:
        client.admin.command("ping")
        print("MongoDB OK")
    finally:
        client.close()


def check_spark_master():
    import json
    from urllib.request import urlopen

    with urlopen("http://spark-master:8080/json/", timeout=5) as response:
        data = json.loads(response.read().decode("utf-8"))
    status = data.get("status", "UNKNOWN")
    print(f"Spark status: {status}")
    if status != "ALIVE":
        raise RuntimeError("Spark Master non disponible")


def check_model_exists():
    from pathlib import Path

    model_path = Path("/opt/airflow/models/best_model")
    if not model_path.exists():
        raise FileNotFoundError("Modele introuvable: /opt/airflow/models/best_model")
    print("Modele best_model detecte")


with DAG(
    dag_id="00_streaming_preflight",
    description="Preflight checks pour pipeline streaming (Kafka/Spark/Mongo/Model)",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["streaming", "ops", "preflight"],
) as dag:
    kafka_check = PythonOperator(
        task_id="check_kafka_and_topic",
        python_callable=check_kafka,
    )

    mongo_check = PythonOperator(
        task_id="check_mongodb",
        python_callable=check_mongodb,
    )

    spark_check = PythonOperator(
        task_id="check_spark_master",
        python_callable=check_spark_master,
    )

    model_check = PythonOperator(
        task_id="check_model_exists",
        python_callable=check_model_exists,
    )

    [kafka_check, mongo_check, spark_check, model_check]

