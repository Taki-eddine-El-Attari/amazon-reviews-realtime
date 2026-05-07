from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="04_dashboard_aggregation",
    description="Agrégation MongoDB pour le dashboard offline — toutes les 10 minutes",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/10 * * * *",   # toutes les 10 minutes
    catchup=False,
    tags=["dashboard", "mongodb", "aggregation"],
) as dag:

    # ── Tâche 1 : Stats globales ───────────────────────────
    def aggregate_global_stats(**context):
        from pymongo import MongoClient
        client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
        db = client["amazon_reviews"]

        total    = db.predictions.count_documents({})
        positive = db.predictions.count_documents({"predicted_label": "positive"})
        negative = db.predictions.count_documents({"predicted_label": "negative"})
        neutral  = db.predictions.count_documents({"predicted_label": "neutral"})

        stats = {
            "computed_at": str(days_ago(0)),
            "total": total,
            "positive": positive,
            "negative": negative,
            "neutral":  neutral,
            "positive_pct": round(positive / max(total, 1) * 100, 2),
            "negative_pct": round(negative / max(total, 1) * 100, 2),
            "neutral_pct":  round(neutral  / max(total, 1) * 100, 2),
        }

        db.dashboard_stats.replace_one(
            {"_id": "global"},
            {"_id": "global", **stats},
            upsert=True
        )

        print("=" * 40)
        print(f"Total      : {total:,}")
        print(f"Positifs   : {positive:,} ({stats['positive_pct']}%)")
        print(f"Négatifs   : {negative:,} ({stats['negative_pct']}%)")
        print(f"Neutres    : {neutral:,}  ({stats['neutral_pct']}%)")
        print("=" * 40)

        client.close()

    global_stats = PythonOperator(
        task_id="aggregate_global_stats",
        python_callable=aggregate_global_stats,
    )

    # ── Tâche 2 : Stats par date ───────────────────────────
    def aggregate_by_date(**context):
        from pymongo import MongoClient
        client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
        db = client["amazon_reviews"]

        pipeline = [
            {"$addFields": {"date": {"$substr": ["$processed_at", 0, 10]}}},
            {"$group": {
                "_id": {"date": "$date", "label": "$predicted_label"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.date": 1}}
        ]

        results = list(db.predictions.aggregate(pipeline))
        print(f"Agrégation par date : {len(results)} entrées")

        # Sauvegarder dans une collection dédiée
        db.stats_by_date.drop()
        if results:
            db.stats_by_date.insert_many(results)

        client.close()

    by_date = PythonOperator(
        task_id="aggregate_by_date",
        python_callable=aggregate_by_date,
    )

    # ── Tâche 3 : Top produits ─────────────────────────────
    def aggregate_top_products(**context):
        from pymongo import MongoClient
        client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
        db = client["amazon_reviews"]

        pipeline = [
            {"$group": {
                "_id": "$product_id",
                "total":    {"$sum": 1},
                "positive": {"$sum": {"$cond": [{"$eq": ["$predicted_label", "positive"]}, 1, 0]}},
                "negative": {"$sum": {"$cond": [{"$eq": ["$predicted_label", "negative"]}, 1, 0]}},
                "neutral":  {"$sum": {"$cond": [{"$eq": ["$predicted_label", "neutral"]},  1, 0]}},
            }},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ]

        results = list(db.predictions.aggregate(pipeline))
        print(f"Top {len(results)} produits calculés")

        db.stats_top_products.drop()
        if results:
            db.stats_top_products.insert_many(results)

        client.close()

    top_products = PythonOperator(
        task_id="aggregate_top_products",
        python_callable=aggregate_top_products,
    )

    # ── Ordre ─────────────────────────────────────────────
    global_stats >> by_date >> top_products