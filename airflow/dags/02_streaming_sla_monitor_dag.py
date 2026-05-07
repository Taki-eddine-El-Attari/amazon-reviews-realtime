from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def evaluate_streaming_sla():
    from pymongo import MongoClient

    client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
    try:
        db = client["amazon_reviews"]
        predictions = db["predictions"]
        reports = db["streaming_sla_reports"]

        now_utc = datetime.now(timezone.utc)
        freshness_threshold = now_utc - timedelta(minutes=5)

        latest = predictions.find_one(sort=[("_id", -1)])
        latest_processed_raw = latest.get("processed_at") if latest else None

        freshness_ok = False
        parsed_latest = None
        if latest_processed_raw:
            parsed_latest = datetime.fromisoformat(
                latest_processed_raw.replace("Z", "+00:00")
            )
            if parsed_latest.tzinfo is None:
                parsed_latest = parsed_latest.replace(tzinfo=timezone.utc)
            freshness_ok = parsed_latest >= freshness_threshold

        recent_count_pipeline = [
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
                    "_processed_at_dt": {
                        "$gte": (now_utc - timedelta(minutes=5)),
                        "$lte": now_utc,
                    }
                }
            },
            {"$count": "count"},
        ]
        recent_count_result = list(predictions.aggregate(recent_count_pipeline))
        recent_count = recent_count_result[0]["count"] if recent_count_result else 0

        anomalies = []
        if not freshness_ok:
            anomalies.append("stale_predictions")
        if recent_count == 0:
            anomalies.append("no_predictions_last_5m")

        report = {
            "computed_at": now_utc.isoformat(),
            "freshness_threshold_utc": freshness_threshold.isoformat(),
            "latest_processed_at": latest_processed_raw,
            "latest_processed_at_parsed": parsed_latest.isoformat() if parsed_latest else None,
            "freshness_ok": freshness_ok,
            "recent_count_last_5m": recent_count,
            "anomalies": anomalies,
            "status": "alert" if anomalies else "ok",
        }

        reports.insert_one(report)
        print(report)

        if anomalies:
            raise RuntimeError(f"SLA alerte: {', '.join(anomalies)}")
    finally:
        client.close()


with DAG(
    dag_id="02_streaming_sla_monitor",
    description="Monitoring SLA et fraicheur des predictions streaming",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["streaming", "sla", "monitoring"],
) as dag:
    sla_check = PythonOperator(
        task_id="evaluate_sla",
        python_callable=evaluate_streaming_sla,
    )

    sla_check
