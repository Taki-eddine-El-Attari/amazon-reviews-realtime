// Initialisation de la base de données MongoDB
db = db.getSiblingDB('amazon_reviews');

// Collection pour stocker les prédictions
db.createCollection('predictions');

// Index pour accélérer les requêtes du dashboard
db.predictions.createIndex({ "timestamp": 1 });
db.predictions.createIndex({ "product_id": 1 });
db.predictions.createIndex({ "prediction": 1 });

print("MongoDB initialisé avec succès !");