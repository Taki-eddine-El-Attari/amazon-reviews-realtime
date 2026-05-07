import json
import logging
import sys
import importlib.util
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime


def _bootstrap_kafka_vendor_six_moves():
    module_name = "kafka.vendor.six"
    if "kafka.vendor.six.moves" in sys.modules:
        return

    six_path = Path(__file__).resolve().parents[1] / ".venv" / "Lib" / "site-packages" / "kafka" / "vendor" / "six.py"
    spec = importlib.util.spec_from_file_location(module_name, six_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    sys.modules["kafka.vendor.six.moves"] = module.moves


_bootstrap_kafka_vendor_six_moves()

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

# ─── Configuration ────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "amazon-reviews"
KAFKA_GROUP_ID          = "amazon-reviews-group"
MONGO_URI               = "mongodb://admin:admin123@localhost:38017/amazon_reviews?authSource=admin"
MONGO_DB                = "amazon_reviews"
MONGO_COLLECTION        = "raw_reviews"

# ─── Connexion MongoDB ────────────────────────────────────
client     = MongoClient(MONGO_URI)
db         = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# ─── Initialisation du Consumer ───────────────────────────
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="earliest",   # lire depuis le début
    enable_auto_commit=True,
    auto_commit_interval_ms=1000
)

def consume_reviews():
    logger.info(f"Consumer démarré — écoute du topic : {KAFKA_TOPIC}")
    received_count = 0

    for message in consumer:
        try:
            review = message.value

            # Ajouter les métadonnées Kafka
            review["kafka_partition"] = message.partition
            review["kafka_offset"]    = message.offset
            review["consumed_at"]     = datetime.utcnow().isoformat()

            # Stocker dans MongoDB
            collection.insert_one(review)

            received_count += 1

            if received_count % 50 == 0:
                logger.info(f"{received_count} avis reçus et stockés dans MongoDB")

            # Afficher un aperçu de chaque message
            logger.debug(
                f"[Partition {message.partition} | Offset {message.offset}] "
                f"Product: {review['product_id']} | "
                f"Score: {review['score']} | "
                f"Label: {review['label']}"
            )

        except Exception as e:
            logger.error(f"Erreur traitement message : {e}")
            continue

if __name__ == "__main__":
    consume_reviews()
