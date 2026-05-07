# 🛒 Amazon Reviews Intelligence

> Système complet d'analyse de sentiment des avis Amazon en temps réel,
> basé sur une architecture Big Data distribuée : Kafka · Spark Streaming · MongoDB · Flask · Airflow

![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.4-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.4.0-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-6.0-47A248?style=flat-square&logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-2.3-000000?style=flat-square&logo=flask&logoColor=white)

---

## 📌 Présentation

Ce projet a été réalisé dans le cadre du Mini-Projet Big Data — IASD 2025/2026.

Il implémente un pipeline complet de traitement de flux de données en temps réel pour
l'analyse de sentiment des avis clients sur des produits alimentaires Amazon.

Le système prédit automatiquement si un avis est **positif**, **neutre** ou **négatif**
et présente les résultats en deux modes :

- **Mode Online** : affichage en continu (temps réel) via Kafka + Spark Streaming
- **Mode Offline** : tableau de bord analytique basé sur les données journalisées dans MongoDB

---

## 🏗️ Architecture

```mermaid
flowchart TD
	A[Amazon Reviews CSV] --> B[Kafka Producer]
	B --> C[Apache Kafka / Zookeeper]
	C --> D[Spark Streaming + MLlib Model]
	D --> E[MongoDB predictions]
	D --> F[Flask Web App]
	E --> F
    I[Apache Airflow] --> C
    I --> D
    I --> E

	F --> G[Flux Online<br/>Socket.IO temps réel]
	F --> H[Dashboard Offline<br/>Graphiques MongoDB]
```

---

## 🛠️ Technologies utilisées

| Technologie | Version | Rôle |
|---|---|---|
| Apache Kafka | 7.4.0 (Confluent) | Streaming des avis en temps réel |
| Apache Zookeeper | 7.4.0 | Gestion du cluster Kafka |
| Apache Spark | 3.4.0 | Traitement distribué + MLlib |
| MongoDB | 6.0 | Stockage NoSQL des prédictions |
| Docker & Compose | latest | Conteneurisation de l'architecture |
| Python | 3.10 | Développement des scripts |
| PySpark MLlib | 3.4.0 | Machine Learning distribué |
| Flask + SocketIO | 2.3.3 | Interface web temps réel |
| Apache Airflow | 2.8.0 | Orchestration, monitoring SLA et qualité |
| NLTK | 3.8 | Lemmatisation du texte |

---

## 📊 Dataset

**Source** : [Amazon Fine Food Reviews — Kaggle](https://www.kaggle.com/snap/amazon-fine-food-reviews)

| Caractéristique | Valeur |
|---|---|
| Nombre d'avis | ~568 000 |
| Période couverte | 1999 – 2012 |
| Fichier | `Reviews.csv` |

**Colonnes principales :**

| Colonne | Description |
|---|---|
| `Id` | Identifiant unique de l'avis |
| `ProductId` | Identifiant du produit |
| `UserId` | Identifiant de l'utilisateur |
| `Score` | Note de 1 à 5 |
| `Summary` | Résumé court de l'avis |
| `Text` | Contenu complet de l'avis |

**Règle de labellisation (cible) :**
Score < 3  →  0  (négatif)
Score = 3  →  1  (neutre)
Score > 3  →  2  (positif)

---

## 🤖 Machine Learning

### Pipeline NLP

```mermaid
flowchart TD
    A[Texte brut] --> B[Nettoyage<br/>minuscules, regex, caractères spéciaux]
    B --> C[Lemmatisation<br/>NLTK WordNetLemmatizer]
    C --> D[Suppression des stopwords]
    D --> E[Tokenisation<br/>Spark Tokenizer]
    E --> F[HashingTF<br/>10 000 features]
    F --> G[IDF<br/>pondération TF-IDF]
    G --> H[Modèle de classification]
    H --> I[Prédiction<br/>négatif / neutre / positif]
```

### Modèles évalués
<img width="1500" height="900" alt="image" src="https://github.com/user-attachments/assets/fb182f1e-d2b2-4548-a4eb-76bf93bfccc5" />
<br></br>

| Modèle | Accuracy (Test) | F1-score |
|---|---|---|
| Naive Bayes | 0.7674 | 0.7945 |
| Logistic Regression | 0.8668 | 0.8503 |
| Random Forest | 0.7821 | 0.6869 |

> Le meilleur modèle est sélectionné automatiquement et sauvegardé dans `models/best_model/`

### Split des données
80% → Entraînement
10% → Validation + Ajustement hyperparamètres
10% → Test final + Mode Online/Offline

---

## 📁 Structure du projet

```text
amazon-reviews-realtime/
│
├── docker-compose.yml          ← Orchestration de tous les services
├── README.md
├── .env                        ← Variables d'environnement
├── .gitignore
│
├── data/
│   └── Reviews.csv             ← Dataset Kaggle (non versionné)
│
├── notebooks/
│   └── 01_data_preparation_and_ml.ipynb  ← Pipeline ML complet
│
├── kafka/
│   ├── producer.py             ← Envoi des avis dans Kafka
│   ├── consumer.py             ← Consommation et stockage
│   ├── create_topic.py         ← Création du topic (3 partitions)
│   └── requirements.txt
│
├── spark/
│   ├── streaming_predict.py    ← Spark Streaming + prédictions
│   ├── test_model_local.py     ← Test local du modèle
│   ├── submit.sh               ← Script de soumission Spark
│   └── requirements.txt
│
├── airflow/
│   ├── dags/
│   │   ├── 00_streaming_preflight_dag.py
│   │   ├── 01_streaming_guardrails_dag.py
│   │   ├── 02_streaming_sla_monitor_dag.py
│   │   └── 03_dashboard_refresh_dag.py
│   └── logs/
│
├── web/
│   ├── app.py                  ← Backend Flask + SocketIO
│   ├── Dockerfile
│   ├── requirements.txt
│   └── templates/
│       ├── base.html           ← Layout commun
│       ├── index.html          ← Flux Online (temps réel)
│       └── dashboard.html      ← Dashboard Offline (analytique)
│
├── mongo/
│   └── init.js                 ← Initialisation MongoDB
│
└── models/
    └── best_model/             ← Modèle PySpark sauvegardé
```

---

## 🚀 Lancement du projet

### Prérequis

- Docker Desktop installé et démarré
- Python 3.10+
- 8 Go RAM minimum recommandés
- Fichier `Reviews.csv` placé dans `data/`

### Étape 1 — Cloner le projet

```bash
git clone https://github.com/VOTRE_USERNAME/amazon-reviews-realtime.git
cd amazon-reviews-realtime
```

### Étape 2 — Placer le dataset

Téléchargez `Reviews.csv` depuis [Kaggle](https://www.kaggle.com/snap/amazon-fine-food-reviews)
et placez-le dans le dossier `data/`.

### Étape 3 — Lancer l'infrastructure Docker

```bash
docker-compose up -d
```

Vérifier que tous les services sont UP :

```bash
docker-compose ps
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8082 |
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8081 |
| Jupyter Notebook | http://localhost:8888 |
| Flask App | http://localhost:5000 |

### Étape 4 — Exécuter le notebook ML

Ouvrez **http://localhost:8888**, naviguez vers `notebooks/` et exécutez
`01_data_preparation_and_ml.ipynb` cellule par cellule.

Le modèle sera sauvegardé dans `models/best_model/`.

### Étape 5 — Créer le topic Kafka

```bash
cd kafka
pip install -r requirements.txt
python create_topic.py
```

### Étape 6 — Lancer Spark Streaming

```bash
bash spark/submit.sh
```

### Étape 7 — Lancer le Producer Kafka

```bash
cd kafka
python producer.py
```

### Étape 8 — Accéder à l'interface web

| Page | URL | Description |
|---|---|---|
| Flux Online | http://localhost:5000 | Prédictions en temps réel |
| Dashboard Offline | http://localhost:5000/dashboard | Analyses MongoDB |
| Airflow UI | http://localhost:8082 | Monitoring des DAGs |

### Étape 9 — DAGs Airflow à activer

- `00_streaming_preflight` : vérification Kafka/Spark/Mongo/modèle
- `01_streaming_guardrails` : contrôles qualité streaming + rapport MongoDB
- `02_streaming_sla_monitor` : contrôle SLA/fraicheur des prédictions + rapport MongoDB
- `04_dashboard_aggregation` : agrégations dashboard offline

---

## 🖥️ Interface Web

### Mode Online — Flux temps réel
<img width="1857" height="931" alt="image" src="https://github.com/user-attachments/assets/44779e1c-86d2-41f7-8639-b5627aca9f3d" />

- Affichage en continu de chaque avis avec sa prédiction
- Badge coloré : 🟢 Positif · 🔴 Négatif · 🟡 Neutre
- Donut chart de distribution live
- Logs système en temps réel
- Refresh automatique toutes les 3 secondes

### Mode Offline — Dashboard analytique
<img width="1864" height="832" alt="image" src="https://github.com/user-attachments/assets/44154bcf-46d0-4a2d-a953-af39dd95ae95" />
<img width="1852" height="838" alt="image" src="https://github.com/user-attachments/assets/d0153b94-88df-464f-9bba-6543f9c2dbc7" />

- KPIs globaux : total, positifs, négatifs, neutres
- Graphique en barres empilées : prédictions par date
- Camembert : scoring par ProductId (ex: `B001E4KFG0`)
- Top 10 produits les plus commentés avec score de positivité
- Refresh automatique toutes les 3 secondes

---

## 🌬️ Airflow Monitoring

### Vue des DAGs

<!-- Remplacer ce placeholder par votre capture écran Airflow -->
![Airflow DAGs - Placeholder](docs/images/airflow-dags-placeholder.png)

- `00_streaming_preflight` : Vérifie que l’environnement est prêt: Kafka + topic, Spark master, MongoDB, et modèle best_model présent.
- `01_streaming_guardrails` : Fait des contrôles qualité streaming (volume, champs manquants, distribution labels) et stocke un rapport santé dans streaming_health_reports.
- `02_streaming_sla_monitor` : Surveille le SLA temps réel (fraîcheur et activité des prédictions), stocke un rapport dans streaming_sla_reports, et échoue si anomalie détectée.
- `04_dashboard_aggregation` : Calcule les agrégations offline pour le dashboard (stats globales, par date, top produits) à partir de MongoDB.

---

## 🔧 Variables d'environnement

Créez un fichier `.env` à la racine :

```env
MONGO_URI=mongodb://admin:admin123@localhost:38017/amazon_reviews?authSource=admin
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=amazon-reviews
```

---

## 👥 Auteurs

| Nom | Rôle |
|---|---|
| EL Attari Taki eddine | Développement complet |

**Encadrant** : Pr. El Yusufi Yasyn

**Module** : Big Data

---

## 📄 Licence

Ce projet est réalisé dans un cadre académique — IASD 2025/2026.
