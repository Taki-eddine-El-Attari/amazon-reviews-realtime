import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, when,
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)
from pyspark.ml import PipelineModel
from pymongo import MongoClient
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkStreaming")

# ─── Configuration ────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC             = "amazon-reviews"
MONGO_URI               = "mongodb://admin:admin123@mongodb:27017/amazon_reviews?authSource=admin"
MONGO_DB                = "amazon_reviews"
MONGO_COLLECTION        = "predictions"
MODEL_PATH              = "/models/best_model"

# ─── Schéma du message Kafka ──────────────────────────────
REVIEW_SCHEMA = StructType([
    StructField("id",           StringType(),  True),
    StructField("product_id",   StringType(),  True),
    StructField("user_id",      StringType(),  True),
    StructField("profile_name", StringType(),  True),
    StructField("score",        IntegerType(), True),
    StructField("summary",      StringType(),  True),
    StructField("text",         StringType(),  True),
    StructField("label",        StringType(),  True),
    StructField("timestamp",    StringType(),  True),
    StructField("unix_time",    StringType(),  True),
])

# ─── Nettoyage texte (même logique que notebook) ──────────
def clean_text(text):
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    tokens = text.split()
    tokens = [w for w in tokens if len(w) > 2]
    return ' '.join(tokens)

clean_udf = udf(clean_text, StringType())

# ─── Mapping prédiction → label ───────────────────────────
def map_prediction(pred):
    mapping = {0.0: "negative", 1.0: "neutral", 2.0: "positive"}
    return mapping.get(pred, "unknown")

map_pred_udf = udf(map_prediction, StringType())

# ─── Écriture dans MongoDB ────────────────────────────────
def write_to_mongo(batch_df, batch_id):
    records = batch_df.collect()
    if not records:
        return

    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    col    = db[MONGO_COLLECTION]

    docs = []
    for row in records:
        doc = {
            "batch_id":          batch_id,
            "id":                row["id"],
            "product_id":        row["product_id"],
            "user_id":           row["user_id"],
            "profile_name":      row["profile_name"],
            "score":             row["score"],
            "summary":           row["summary"],
            "text":              row["text"],
            "clean_text":        row["clean_text"],
            "real_label":        row["label"],
            "predicted_label":   row["predicted_label"],
            "prediction_code":   float(row["prediction"]),
            "timestamp":         row["timestamp"],
            "unix_time":         row["unix_time"],
            "processed_at":      datetime.utcnow().isoformat(),
        }
        docs.append(doc)

    if docs:
        col.insert_many(docs)
        logger.info(f"Batch {batch_id} — {len(docs)} prédictions insérées dans MongoDB")

    client.close()

# ─── Spark Session ────────────────────────────────────────
def create_spark_session():
    return SparkSession.builder \
        .appName("AmazonReviewsStreaming") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()

# ─── Main ─────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session démarrée")

    # Charger le modèle sauvegardé
    model = PipelineModel.load(MODEL_PATH)
    logger.info(f"Modèle chargé depuis : {MODEL_PATH}")

    # Lire le flux Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Désérialiser le JSON
    parsed_stream = raw_stream.select(
        from_json(
            col("value").cast("string"),
            REVIEW_SCHEMA
        ).alias("data")
    ).select("data.*")

    # Nettoyer le texte
    cleaned_stream = parsed_stream.withColumn(
        "clean_text", clean_udf(col("text"))
    ).filter(col("clean_text") != "")

    # Appliquer le modèle
    predictions = model.transform(cleaned_stream)

    # Ajouter le label prédit lisible
    result_stream = predictions.withColumn(
        "predicted_label", map_pred_udf(col("prediction"))
    ).select(
        "id", "product_id", "user_id", "profile_name",
        "score", "summary", "text", "clean_text",
        "label", "prediction", "predicted_label",
        "timestamp", "unix_time"
    )

    # Écrire dans MongoDB par micro-batch
    query = result_stream.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info("Spark Streaming démarré — en attente de messages Kafka...")
    query.awaitTermination()

if __name__ == "__main__":
    main()