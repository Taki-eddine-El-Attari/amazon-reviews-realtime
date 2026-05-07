from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def run_streaming_guardrails():
    from pymongo import MongoClient

    client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
    try:
        db = client["amazon_reviews"]
        predictions = db["predictions"]

        now_utc = datetime.now(timezone.utc)
        window_start = now_utc - timedelta(minutes=10)

        total_10m_pipeline = [
            {
                "$addFields": {
                    "_processed_at_dt": {
                        "$dateFromString": {
                            "dateString": "$processed_at",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"_processed_at_dt": {"$gte": window_start}}},
            {"$count": "count"},
        ]
        total_10m_result = list(predictions.aggregate(total_10m_pipeline))
        total_10m = total_10m_result[0]["count"] if total_10m_result else 0

        missing_required = predictions.count_documents(
            {
                "$or": [
                    {"predicted_label": {"$exists": False}},
                    {"predicted_label": None},
                    {"product_id": {"$exists": False}},
                    {"product_id": None},
                    {"processed_at": {"$exists": False}},
                    {"processed_at": None},
                ]
            }
        )

        label_pipeline = [
            {
                "$addFields": {
                    "_processed_at_dt": {
                        "$dateFromString": {
                            "dateString": "$processed_at",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {
                "$match": {
                    "_processed_at_dt": {"$gte": window_start},
                    "predicted_label": {"$in": ["positive", "negative", "neutral"]},
                }
            },
            {"$group": {"_id": "$predicted_label", "count": {"$sum": 1}}},
        ]
        label_distribution = {
            row["_id"]: row["count"] for row in predictions.aggregate(label_pipeline)
        }

        positive_ratio = 0.0
        if total_10m > 0:
            positive_ratio = round(
                label_distribution.get("positive", 0) / total_10m, 4
            )

        anomalies = []
        if total_10m == 0:
            anomalies.append("no_events_last_10m")
        if missing_required > 0:
            anomalies.append("missing_required_fields")
        if total_10m >= 50 and positive_ratio > 0.98:
            anomalies.append("label_distribution_skew_positive")

        report = {
            "computed_at": now_utc.isoformat(),
            "window_minutes": 10,
            "total_10m": total_10m,
            "missing_required_fields_count": missing_required,
            "label_distribution": label_distribution,
            "positive_ratio": positive_ratio,
            "anomalies": anomalies,
            "status": "alert" if anomalies else "ok",
        }

        db["streaming_health_reports"].insert_one(report)
        print(report)
    finally:
        client.close()


with DAG(
    dag_id="01_streaming_guardrails",
    description="Controles qualite streaming et rapport guardrails",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["streaming", "quality", "monitoring"],
) as dag:
    guardrails_task = PythonOperator(
        task_id="run_guardrails_checks",
        python_callable=run_streaming_guardrails,
    )

    guardrails_task
