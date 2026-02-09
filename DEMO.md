# 🎯 Guide de Démonstration - E-Commerce Analytics Platform

> **Durée estimée : 15-20 minutes**
> Ce guide vous accompagne à travers une démo complète du projet.

---

## 🔧 Pré-requis (avant la démo)

```bash
# 1. Lancer Docker Desktop
# 2. Démarrer tous les services
docker-compose up -d

# 3. Charger les données
python ingestion/load_olist_simple.py

# 4. Lancer le dashboard
streamlit run visualization/streamlit_app.py --server.port 8501
```

---

## 📌 PARTIE 1 : Présentation de l'Architecture (2 min)

### Montrer le `docker-compose.yml`
- **11 services** conteneurisés
- Architecture **polyglot persistence** : chaque base de données est choisie pour ses forces

| Base | Modèle | Cas d'usage |
|------|--------|-------------|
| MongoDB | Document | Produits avec schéma flexible, avis textuels |
| ClickHouse | Colonnes (OLAP) | Agrégations rapides sur millions de lignes de ventes |
| Neo4j | Graphe | Relations clients → produits, recommandations |

### Montrer les containers actifs
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

## 📌 PARTIE 2 : Le Dataset Olist (2 min)

Ouvrir un terminal et montrer :
```bash
# 9 fichiers CSV, ~120 MB, dataset réel brésilien
dir data\raw\*.csv

# 99 441 commandes, 32 951 produits, 3 095 vendeurs
# Période : 2016-2018
```

**Point clé** : Ce n'est pas un dataset synthétique — c'est un vrai dataset e-commerce publié par Olist sur Kaggle.

---

## 📌 PARTIE 3 : MongoDB - Document Store (3 min)

### Ouvrir Mongo Express : http://localhost:8091

Montrer :
1. La base `ecommerce` avec les collections `products` et `reviews`
2. Cliquer sur un document produit → schéma flexible JSON
3. Cliquer sur un avis → texte libre + score

### Ou via le terminal :
```bash
docker exec bootcamptalan-mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin --eval "
  db = db.getSiblingDB('ecommerce');
  print('=== Produits ===');
  printjson(db.products.findOne());
  print('\n=== Avis clients ===');
  printjson(db.reviews.findOne());
  print('\n=== Stats ===');
  print('Produits: ' + db.products.countDocuments());
  print('Avis: ' + db.reviews.countDocuments());
"
```

**Point clé** : MongoDB est idéal pour les documents semi-structurés. Un produit peut avoir des attributs variables selon sa catégorie.

---

## 📌 PARTIE 4 : ClickHouse - Analytics OLAP (3 min)

### Requêtes analytiques ultra-rapides

```bash
# Top 10 des catégories par chiffre d'affaires
docker exec bootcamptalan-clickhouse-1 clickhouse-client --query "
  SELECT 
    product_category AS category,
    count() AS nb_ventes,
    round(sum(price), 2) AS chiffre_affaires,
    round(avg(price), 2) AS panier_moyen
  FROM ecommerce.sales_fact
  WHERE product_category != 'unknown'
  GROUP BY category
  ORDER BY chiffre_affaires DESC
  LIMIT 10
  FORMAT PrettyCompact
"

# Ventes par mois (time series)
docker exec bootcamptalan-clickhouse-1 clickhouse-client --query "
  SELECT 
    toYYYYMM(order_purchase_timestamp) AS mois,
    count() AS nb_commandes,
    round(sum(price), 2) AS revenue
  FROM ecommerce.sales_fact
  GROUP BY mois
  ORDER BY mois
  FORMAT PrettyCompact
"

# Répartition géographique (par état brésilien)
docker exec bootcamptalan-clickhouse-1 clickhouse-client --query "
  SELECT 
    customer_state AS etat,
    count() AS nb_commandes,
    round(avg(review_score), 2) AS note_moyenne,
    round(sum(price), 2) AS chiffre_affaires
  FROM ecommerce.sales_fact
  WHERE customer_state != 'unknown'
  GROUP BY etat
  ORDER BY nb_commandes DESC
  LIMIT 10
  FORMAT PrettyCompact
"

# KPIs globaux
docker exec bootcamptalan-clickhouse-1 clickhouse-client --query "
  SELECT 
    count() AS total_ventes,
    round(sum(price), 2) AS ca_total,
    round(avg(price), 2) AS prix_moyen,
    uniq(customer_id) AS clients_uniques,
    round(avg(review_score), 2) AS note_moyenne
  FROM ecommerce.sales_fact
  FORMAT PrettyCompact
"
```

**Point clé** : ClickHouse exécute ces agrégations en **millisecondes** vs secondes pour un RDBMS classique. Conçu pour l'analytics.

---

## 📌 PARTIE 5 : Neo4j - Graph Database (3 min)

### Ouvrir Neo4j Browser : http://localhost:7474
- Login : `neo4j` / `password123`

### Requêtes visuelles (copier dans le Neo4j Browser) :

```cypher
// Visualiser le graphe : Clients qui ont acheté des produits
MATCH (c:Customer)-[r:PURCHASED]->(p:Product)
RETURN c, r, p LIMIT 50
```

```cypher
// Clients qui ont acheté le même produit (base pour recommandations)
MATCH (c1:Customer)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(c2:Customer)
WHERE c1 <> c2
RETURN c1.customer_id AS client1, c2.customer_id AS client2, 
       p.product_id AS produit_commun
LIMIT 20
```

```cypher
// Stats du graphe
MATCH (n) RETURN labels(n)[0] AS type, count(n) AS nombre
```

**Point clé** : Neo4j permet de traverser les relations en O(1), idéal pour les recommandations et l'analyse de réseau.

---

## 📌 PARTIE 6 : Apache Airflow - Orchestration (2 min)

### Ouvrir Airflow : http://localhost:8080
- Login : `admin` / `admin`

Montrer :
1. Le DAG `ecommerce_etl` avec ses **9 tâches**
2. Le graphe de dépendances (vue Graph)
3. Les phases : **Extract → Transform → Load → Spark ELT → Verify**

### Les 9 tâches du pipeline :
| # | Tâche | Description |
|---|-------|-------------|
| 1 | `extract_validate` | Vérification des CSV bruts |
| 2 | `transform_clean` | Nettoyage, déduplication, types |
| 3 | `load_mongodb` | Chargement documents |
| 4 | `load_clickhouse` | Chargement OLAP |
| 5 | `load_neo4j` | Chargement graphe |
| 6 | `spark_batch_analytics` | Agrégations Spark (4 parquets) |
| 7 | `spark_ml_recommendations` | Reco ALS (Spark MLlib) |
| 8 | `verify_mongodb` | Vérification post-load |
| 9 | `verify_clickhouse` | Vérification post-load |

**Point clé** : Airflow permet de planifier, monitorer et relancer automatiquement chaque étape du pipeline.

---

## 📌 PARTIE 7 : Apache Spark - Traitement batch & ML (2 min)

### Spark UI : http://localhost:8082

Montrer les jobs Spark exécutés.

### Ce que Spark produit :

```bash
# 4 fichiers Parquet d'analytics :
# - sales_by_state/     → Ventes par état brésilien
# - sales_by_month/     → Évolution temporelle
# - product_by_category/→ Performance par catégorie
# - customer_segments/  → Segmentation RFM (Récence, Fréquence, Montant)

# + 1 fichier ML :
# - recommendations/    → Recommandations produits (algorithme ALS)
```

**Point clé** : Spark traite le dataset complet en mémoire et produit des résultats Parquet optimisés.

---

## 📌 PARTIE 8 : Dashboard Streamlit (2 min)

### Ouvrir : http://localhost:8501

Le dashboard affiche :
- 📊 KPIs principaux (total commandes, CA, panier moyen)
- 📈 Graphique des ventes par mois
- 🗺️ Répartition géographique par état
- 📦 Top catégories produits
- ⭐ Distribution des notes clients

---

## 📌 PARTIE 9 : MinIO - Data Lake S3 (1 min)

### Ouvrir : http://localhost:9001
- Login : `minioadmin` / `minioadmin`

Montrer que MinIO est prêt comme data lake local compatible S3 pour stocker les fichiers Parquet.

---

## 🎤 Points clés pour la Q&A

1. **Pourquoi polyglot persistence ?** → Chaque base excelle dans son domaine. MongoDB pour la flexibilité, ClickHouse pour la vitesse analytique, Neo4j pour les relations.

2. **Pourquoi Airflow ?** → Orchestration avec retry, monitoring, scheduling. Production-ready.

3. **Pourquoi Spark ?** → Traitement distribué scalable. Passe de 100K à des millions de lignes sans changer le code.

4. **Pourquoi Docker ?** → Reproductibilité. Un seul `docker-compose up` et tout est prêt.

5. **Dataset réel** → Pas de données synthétiques, vrai dataset e-commerce brésilien.

---

## 🔗 Liens rapides pendant la démo

| Service | URL |
|---------|-----|
| Mongo Express | http://localhost:8091 |
| Neo4j Browser | http://localhost:7474 |
| Airflow | http://localhost:8080 |
| Spark Master UI | http://localhost:8082 |
| Streamlit Dashboard | http://localhost:8501 |
| MinIO Console | http://localhost:9001 |
| ClickHouse HTTP | http://localhost:8123 |
