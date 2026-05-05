from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

MODEL_PATH = "../models/best_model"

spark = SparkSession.builder \
    .appName("TestModelLocal") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Charger le modèle
model = PipelineModel.load(MODEL_PATH)
print("Modèle chargé")

# Nettoyage texte
def clean_text(text):
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return ' '.join([w for w in text.split() if len(w) > 2])

clean_udf = udf(clean_text, StringType())

def map_prediction(pred):
    return {0.0: "negative", 1.0: "neutral", 2.0: "positive"}.get(pred, "unknown")

map_udf = udf(map_prediction, StringType())

# Quelques avis de test
test_reviews = [
    ("1", "B001E4KFG0", "This product is absolutely amazing! Best purchase ever.", 5),
    ("2", "B001E4KFG0", "Terrible quality, completely disappointed.", 1),
    ("3", "B001E4KFG0", "It is okay, nothing special about it.", 3),
    ("4", "B002QWP89S", "Loved it! Will buy again for sure.", 5),
    ("5", "B002QWP89S", "Worst product I have ever bought.", 1),
]

df = spark.createDataFrame(
    test_reviews,
    ["id", "product_id", "text", "score"]
)

df = df.withColumn("clean_text", clean_udf("text"))

predictions = model.transform(df)
predictions = predictions.withColumn("predicted_label", map_udf("prediction"))

print("\n=== Résultats de prédiction ===")
predictions.select(
    "id", "text", "score", "predicted_label"
).show(truncate=50)

spark.stop()