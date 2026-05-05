from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import threading
import logging
import os
import sys
import importlib.util
from pathlib import Path


def _bootstrap_kafka_vendor_six_moves():
    module_name = "kafka.vendor.six"
    if "kafka.vendor.six.moves" in sys.modules:
        return

    repo_root = Path(__file__).resolve().parents[1]
    six_path = repo_root / ".venv" / "Lib" / "site-packages" / "kafka" / "vendor" / "six.py"
    spec = importlib.util.spec_from_file_location(module_name, six_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    sys.modules["kafka.vendor.six.moves"] = module.moves


_bootstrap_kafka_vendor_six_moves()

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlaskApp")

app = Flask(__name__)
app.config["SECRET_KEY"] = "amazon-bigdata-2026"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ─── MongoDB ──────────────────────────────────────────────
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/amazon_reviews?authSource=admin")
client     = MongoClient(MONGO_URI)
db         = client["amazon_reviews"]
col_pred   = db["predictions"]

# ─── Kafka ────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = "amazon-reviews"

# ─── Routes principales ───────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

# ─── API : statistiques globales ─────────────────────────
@app.route("/api/stats")
def api_stats():
    total = col_pred.count_documents({})
    positive = col_pred.count_documents({"predicted_label": "positive"})
    neutral  = col_pred.count_documents({"predicted_label": "neutral"})
    negative = col_pred.count_documents({"predicted_label": "negative"})

    return jsonify({
        "total":    total,
        "positive": positive,
        "neutral":  neutral,
        "negative": negative,
        "accuracy_pct": round((positive + negative + neutral) / max(total, 1) * 100, 1)
    })

# ─── API : prédictions par date ──────────────────────────
@app.route("/api/predictions-by-date")
def api_predictions_by_date():
    pipeline = [
        {
            "$addFields": {
                "date_str": {
                    "$substr": ["$processed_at", 0, 10]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "date":  "$date_str",
                    "label": "$predicted_label"
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id.date": 1}},
        {"$limit": 200}
    ]
    results = list(col_pred.aggregate(pipeline))
    formatted = {}
    for r in results:
        date  = r["_id"]["date"]
        label = r["_id"]["label"]
        if date not in formatted:
            formatted[date] = {"positive": 0, "neutral": 0, "negative": 0}
        formatted[date][label] = r["count"]

    return jsonify([
        {"date": k, **v} for k, v in sorted(formatted.items())
    ])

# ─── API : scoring par produit ────────────────────────────
@app.route("/api/product-scoring")
def api_product_scoring():
    product_id = request.args.get("product_id", "B001E4KFG0")
    pipeline = [
        {"$match": {"product_id": product_id}},
        {
            "$group": {
                "_id": "$predicted_label",
                "count": {"$sum": 1}
            }
        }
    ]
    results = list(col_pred.aggregate(pipeline))
    data = {"positive": 0, "neutral": 0, "negative": 0}
    for r in results:
        data[r["_id"]] = r["count"]
    return jsonify({"product_id": product_id, "scoring": data})

# ─── API : top produits ───────────────────────────────────
@app.route("/api/top-products")
def api_top_products():
    pipeline = [
        {
            "$group": {
                "_id": "$product_id",
                "total": {"$sum": 1},
                "positive": {
                    "$sum": {"$cond": [{"$eq": ["$predicted_label", "positive"]}, 1, 0]}
                },
                "negative": {
                    "$sum": {"$cond": [{"$eq": ["$predicted_label", "negative"]}, 1, 0]}
                }
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": 10}
    ]
    results = list(col_pred.aggregate(pipeline))
    for r in results:
        r["product_id"] = r.pop("_id")
        r["score_pct"] = round(r["positive"] / max(r["total"], 1) * 100, 1)
    return jsonify(results)

# ─── API : dernières prédictions ─────────────────────────
@app.route("/api/latest")
def api_latest():
    docs = list(
        col_pred.find({}, {"_id": 0})
        .sort("processed_at", -1)
        .limit(20)
    )
    return jsonify(docs)

# ─── SocketIO : flux temps réel ───────────────────────────
def kafka_listener():
    """Thread qui écoute Kafka et émet via SocketIO"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            group_id="flask-realtime-group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True
        )
        logger.info("✅ Kafka listener démarré")

        for message in consumer:
            review = message.value
            # Chercher la prédiction dans MongoDB
            pred_doc = col_pred.find_one(
                {"id": review.get("id")},
                {"_id": 0}
            )
            if pred_doc:
                socketio.emit("new_prediction", pred_doc)
            else:
                # Émettre le message brut si pas encore prédit
                socketio.emit("new_review", review)

    except Exception as e:
        logger.error(f"❌ Kafka listener erreur : {e}")

@socketio.on("connect")
def on_connect():
    logger.info("🔌 Client connecté")
    # Envoyer les 5 dernières prédictions au nouveau client
    latest = list(
        col_pred.find({}, {"_id": 0})
        .sort("processed_at", -1)
        .limit(5)
    )
    for doc in reversed(latest):
        emit("new_prediction", doc)

# ─── Lancement ────────────────────────────────────────────
if __name__ == "__main__":
    # Démarrer le listener Kafka dans un thread
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()

    socketio.run(app, host="0.0.0.0", port=5000, debug=False)