import csv
import json
import time
import logging
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

# ─── Configuration ────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "amazon-reviews"
DATA_PATH               = "../data/Reviews.csv"
DELAY_SECONDS           = 0.5   # délai entre chaque message (simulation temps réel)

# ─── Initialisation du Producer ───────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",                  # attendre confirmation de tous les brokers
    retries=3,
    linger_ms=10
)

def assign_label(score):
    """Règle du prof : score < 3 → négatif, = 3 → neutre, > 3 → positif"""
    score = int(score)
    if score < 3:
        return "negative"
    elif score == 3:
        return "neutral"
    else:
        return "positive"

def send_reviews():
    logger.info(f"Démarrage du producer sur le topic : {KAFKA_TOPIC}")
    sent_count = 0

    with open(DATA_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # Construire le message
                message = {
                    "id":           row.get("Id", ""),
                    "product_id":   row.get("ProductId", ""),
                    "user_id":      row.get("UserId", ""),
                    "profile_name": row.get("ProfileName", ""),
                    "score":        int(row.get("Score", 0)),
                    "summary":      row.get("Summary", ""),
                    "text":         row.get("Text", ""),
                    "label":        assign_label(row.get("Score", 3)),
                    "timestamp":    datetime.utcnow().isoformat(),
                    "unix_time":    row.get("Time", "")
                }

                # Envoyer dans Kafka avec ProductId comme clé
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=message["product_id"],
                    value=message
                )

                sent_count += 1

                if sent_count % 100 == 0:
                    producer.flush()
                    logger.info(f"{sent_count} avis envoyés...")

                time.sleep(DELAY_SECONDS)

            except Exception as e:
                logger.error(f"Erreur sur la ligne {sent_count} : {e}")
                continue

    producer.flush()
    producer.close()
    logger.info(f"Producer terminé. Total envoyé : {sent_count} avis")

if __name__ == "__main__":
    send_reviews()