# E-Commerce Analytics - Pipeline ETL/ELT avec NoSQL, Spark & Airflow

Plateforme d'analytics e-commerce utilisant une architecture **polyglot persistence** (MongoDB, ClickHouse, Neo4j), orchestrée par **Apache Airflow** avec du traitement batch et ML par **Apache Spark**, sur le dataset réel **Olist Brazilian E-Commerce** (99 441 commandes).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│               SOURCES : Dataset Olist (CSV, ~120 MB)        │
│   orders · customers · products · order_items · reviews     │
│   payments · sellers                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │     ORCHESTRATION : Airflow     │
        │        DAG ecommerce_etl        │
        └────────────────┬────────────────┘
                         │
     ┌───────────────────┼───────────────────┐
     ▼                   ▼                   ▼
 ┌────────┐        ┌──────────┐        ┌──────────┐
 │EXTRACT │        │TRANSFORM │        │   LOAD   │
 │Validate│───────▶│ Clean    │───────▶│ MongoDB  │
 │CSV data│        │ Dedup    │        │ClickHouse│
 └────────┘        │ Types    │        │  Neo4j   │
                   └──────────┘        └─────┬────┘
                                             │
                                    ┌────────▼────────┐
                                    │  SPARK (ELT)    │
                                    │  Batch + ML     │
                                    │  (local[*])     │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
                                    │  VISUALIZATION  │
                                    │  Streamlit      │
                                    └─────────────────┘
```

---

## Technologies

| Technologie | Rôle | Port |
|---|---|---|
| **MongoDB 7.0** | Document store (produits, avis) | 27017 |
| **ClickHouse 23.8** | OLAP analytics (ventes) | 8123 |
| **Neo4j 5.13** | Graph DB (relations clients/produits) | 7474, 7687 |
| **Spark 3.4** | Batch processing + ML (ALS) | 8082, 7077 |
| **Airflow 2.8** | Orchestration pipeline ETL/ELT | 8080 |
| **Streamlit** | Dashboard interactif | 8501 |
| **Cassandra 4.1** | Wide-column store (inactif - Python 3.12 incompatible) | 9042 |
| **Kafka** | Event streaming (infrastructure provisionnée) | 9092 |
| **Flink 1.18** | Stream processing (infrastructure provisionnée) | 8081 |
| **Trino 432** | Distributed SQL (infrastructure provisionnée) | 8083 |
| **Redis Stack** | Cache + TimeSeries (infrastructure provisionnée) | 6379 |
| **Superset 3.0** | BI dashboards (infrastructure provisionnée) | 8088 |
| **MinIO** | S3-compatible data lake (infrastructure provisionnée) | 9001 |

---

## Dataset : Olist Brazilian E-Commerce

| Fichier | Contenu | Lignes |
|---|---|---|
| `olist_orders_dataset.csv` | Commandes | 99 441 |
| `olist_customers_dataset.csv` | Clients | 99 441 |
| `olist_products_dataset.csv` | Produits | 32 951 |
| `olist_order_items_dataset.csv` | Lignes commande | 112 650 |
| `olist_order_reviews_dataset.csv` | Avis clients | 99 224 |
| `olist_order_payments_dataset.csv` | Paiements | 103 886 |
| `olist_sellers_dataset.csv` | Vendeurs | 3 095 |

Période couverte : 2016-2018. Taille totale : ~120 MB.

---

## Structure du Projet

```
bootcamp talan/
├── docker-compose.yml              # Infrastructure Docker (19 services)
├── requirements.txt                # Dépendances Python
├── README.md                       # Cette documentation
├── .env.example                    # Template variables d'environnement
├── .gitignore
├── start.bat / start.sh            # Scripts de démarrage rapide
│
├── config/                         # Schémas des bases de données
│   ├── mongodb_schema.js
│   ├── clickhouse_schema.sql
│   ├── neo4j_schema.cypher
│   ├── cassandra_schema.cql
│   └── trino/                      # Configuration Trino
│
├── data/
│   ├── raw/                        # CSV bruts Kaggle (9 fichiers)
│   ├── cleaned/                    # CSV nettoyés (Transform)
│   └── parquet/                    # Sorties Spark (5 dossiers Parquet)
│
├── scripts/
│   └── download_dataset.py         # Téléchargement automatique Kaggle
│
├── ingestion/
│   └── load_olist_simple.py        # Chargement vers MongoDB, ClickHouse, Neo4j
│
├── processing/
│   ├── spark_batch.py              # Agrégations + segmentation RFM
│   └── spark_ml_recommendations.py # Recommandations ALS (Spark MLlib)
│
├── orchestration/
│   ├── airflow_dags/
│   │   └── ecommerce_pipeline.py   # DAG ETL/ELT complet (9 tasks)
│   └── airflow_logs/
│
├── analytics/
│   └── queries.md                  # Collection de requêtes (MongoDB, ClickHouse, Neo4j)
│
├── visualization/
│   └── streamlit_app.py            # Dashboard 4 pages
│
├── tests/
│   └── test_integration.py         # Tests d'intégration
│
└── reports/                        # Rapports générés par le pipeline
```

---

## Installation et Démarrage

### Prérequis

- Docker Desktop (avec Docker Compose)
- Python 3.9+
- Compte Kaggle (gratuit) avec API token
- 10 GB d'espace disque, 8 GB RAM minimum

### 1. Configurer Kaggle API

```bash
# Windows
mkdir %USERPROFILE%\.kaggle
# Placer kaggle.json dans ce dossier

# Linux/Mac
mkdir ~/.kaggle && mv kaggle.json ~/.kaggle/ && chmod 600 ~/.kaggle/kaggle.json
```

### 2. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

### 3. Démarrer l'infrastructure Docker

```bash
docker-compose up -d
docker-compose ps              # Vérifier que tous les services sont Up
```

Temps de démarrage : ~3-5 minutes (première fois : 10-15 min pour les images).

### 4. Télécharger le dataset

```bash
python scripts/download_dataset.py
```

### 5. Charger les données

```bash
python ingestion/load_olist_simple.py
```

### 6. Pipeline Airflow (optionnel)

Le DAG `ecommerce_etl_pipeline` s'exécute automatiquement ou peut être déclenché via l'interface :

- Ouvrir http://localhost:8080 (admin / admin)
- Activer le DAG `ecommerce_etl_pipeline`
- Cliquer "Trigger DAG"

### 7. Dashboard Streamlit

```bash
streamlit run visualization/streamlit_app.py
```

Ouvrir http://localhost:8501

---

## Pipeline ETL/ELT (DAG Airflow)

Le pipeline `ecommerce_etl_pipeline` comprend **9 tâches** en 5 phases :

```
EXTRACT                    TRANSFORM              LOAD
┌──────────────────┐      ┌──────────────┐      ┌─────────────────┐
│ check_data       │─────▶│ clean_data   │─────▶│ load_databases  │
│ validate_quality │      │ (7 CSV)      │      │ (Mongo/CH/Neo4j)│
└──────────────────┘      └──────────────┘      └────────┬────────┘
                                                         │
                                               ┌─────────┴─────────┐
                                               ▼                   ▼
                                         ┌───────────┐      ┌───────────┐
SPARK ELT                                │ Batch     │      │ ML ALS    │
                                         │ Processing│      │ Recomm.   │
                                         └─────┬─────┘      └─────┬─────┘
                                               └──────┬───────────┘
                                                      ▼
VERIFY                                         ┌─────────────┐
                                               │ verify      │
                                               │ summary     │
                                               │ cleanup     │
                                               └─────────────┘
```

### Phase 1 - Extract
- **extract_check_data** : Vérifie la présence des CSV dans `data/raw/`
- **extract_validate_quality** : Compte les lignes, nulls, doublons pour les 7 fichiers

### Phase 2 - Transform
- **transform_clean_data** : Nettoyage des 7 datasets (suppression doublons, conversion types dates/numériques, standardisation villes/états, remplissage nulls). Sortie dans `data/cleaned/`

### Phase 3 - Load
- **load_to_databases** : Exécute `load_olist_simple.py` vers MongoDB (produits, avis), ClickHouse (ventes), Neo4j (graphe clients/produits)

### Phase 4 - Spark ELT (en parallèle)
- **spark_batch_processing** : Agrégations par état/mois, métriques produits, segmentation RFM. Sorties Parquet : `sales_by_state`, `sales_by_month`, `product_by_category`, `customer_segments`
- **spark_ml_recommendations** : Modèle ALS (Alternating Least Squares) avec Spark MLlib. Sortie Parquet : `recommendations`

Les jobs Spark sont exécutés via Docker Python SDK (`docker.from_env()`) depuis Airflow, en mode `local[*]` sur le conteneur spark-master.

### Phase 5 - Verify
- **verify_results** : Vérifie les fichiers Parquet et cleaned
- **generate_summary** : Génère un rapport d'exécution dans `reports/`
- **cleanup_temp_files** : Nettoyage final

---

## Bases de Données - Rôles et Données

### MongoDB (Document Store)
- **Collections** : `products`, `reviews`
- **Usage** : Catalogue produits flexible, avis clients avec notes
- **Requête type** : Distribution des notes, produits par catégorie

### ClickHouse (OLAP Analytics)
- **Table** : `ecommerce.sales_fact`
- **Usage** : Analytics haute performance (KPIs, revenue, tendances)
- **Requête type** : Ventes quotidiennes, top catégories, panier moyen

### Neo4j (Graph Database)
- **Noeuds** : `Customer`, `Product`
- **Relations** : `PURCHASED`
- **Usage** : Recommandations collaboratives, market basket analysis
- **Requête type** : "Les clients qui ont acheté X ont aussi acheté Y"

### Cassandra (inactif)
- Incompatibilité driver Python 3.12. Schéma prêt dans `config/cassandra_schema.cql`.

---

## Spark Processing

### Batch (`spark_batch.py`)
- Ventes agrégées par état brésilien
- Ventes mensuelles (tendances)
- Métriques produits par catégorie (nombre, prix moyen)
- Segmentation RFM (Recency, Frequency, Monetary) clients
- **Sortie** : 4 dossiers Parquet dans `data/parquet/`

### ML Recommendations (`spark_ml_recommendations.py`)
- Modèle ALS (Alternating Least Squares) via Spark MLlib
- Rating composite : `review_score * (1 + log1p(price) / 10)`
- Split train/test 80/20, évaluation RMSE
- Top 5 recommandations par client
- **Sortie** : `data/parquet/recommendations/`

---

## Dashboard Streamlit (4 pages)

| Page | Contenu | Source |
|---|---|---|
| **Vue d'Ensemble** | Compteurs (commandes, clients, revenue), évolution ventes, distribution avis | MongoDB + ClickHouse |
| **Analyse des Ventes** | Panier moyen, tendances, top catégories | ClickHouse |
| **Analyse des Avis** | Note moyenne (4.05/5), distribution scores, satisfaction | MongoDB |
| **Graphe & Recommandations** | Top clients actifs, recommandations collaboratives | Neo4j |

---

## Interfaces Web

| Service | URL | Identifiants |
|---|---|---|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Spark Master** | http://localhost:8082 | - |
| **Neo4j Browser** | http://localhost:7474 | neo4j / password123 |
| **Mongo Express** | http://localhost:8091 | admin / password123 |
| **Streamlit** | http://localhost:8501 | - |
| **Kafka UI** | http://localhost:8090 | - |
| **Flink Dashboard** | http://localhost:8081 | - |
| **RedisInsight** | http://localhost:8001 | - |
| **Trino** | http://localhost:8083 | - |
| **Superset** | http://localhost:8088 | - |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |

---

## Commandes Utiles

```bash
# Démarrer / arrêter l'infrastructure
docker-compose up -d
docker-compose down

# Voir les logs d'un service
docker-compose logs -f spark-master

# Lancer le pipeline manuellement (depuis le host)
docker exec -u airflow bootcamptalan-airflow-webserver-1 \
  airflow dags trigger ecommerce_etl_pipeline

# Lancer Spark batch directement
docker exec bootcamptalan-spark-master-1 \
  /opt/spark/bin/spark-submit --driver-memory 1g \
  /opt/spark-apps/processing/spark_batch.py

# Arrêter et supprimer toutes les données
docker-compose down -v
```

---

## Auteurs

Bootcamp Talan - Data Engineering Project

## Licence

MIT
