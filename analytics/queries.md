# Requêtes Analytiques - E-Commerce

Collection de requêtes pour analyser les données Olist Brazilian E-Commerce.

**Bases actives** : MongoDB, ClickHouse, Neo4j  
**Bases inactives** : Cassandra (incompatibilité Python 3.12), Trino (infrastructure provisionnée)

---

## CASSANDRA (CQL) - Reference Only (inactif)

### Ventes par Date
```cql
SELECT order_date, COUNT(*) as total_orders, SUM(total_amount) as revenue
FROM orders_by_date
WHERE order_date >= '2018-01-01' AND order_date <= '2018-12-31'
GROUP BY order_date
ORDER BY order_date DESC;
```

### Commandes par Client
```cql
SELECT customer_id, COUNT(*) as order_count, SUM(total_amount) as lifetime_value
FROM orders_by_customer
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 100;
```

### Commandes par Statut
```cql
SELECT order_status, COUNT(*) as count
FROM orders_by_date
GROUP BY order_status;
```

### Ventes par Mois
```cql
SELECT year_month, product_id, SUM(price) as total_sales
FROM product_sales
WHERE year_month = '2018-01'
GROUP BY year_month, product_id
ORDER BY total_sales DESC
LIMIT 20;
```

---

## 🟢 MONGODB (JavaScript)

### Top 10 Catégories
```javascript
db.products.aggregate([
  { $group: { _id: "$product_category_name", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 10 }
]);
```

### Distribution des Notes
```javascript
db.reviews.aggregate([
  { $group: { _id: "$review_score", count: { $sum: 1 } } },
  { $sort: { _id: 1 } }
]);
```

### Reviews avec Commentaires
```javascript
db.reviews.find({
  review_comment_message: { $exists: true, $ne: null },
  review_score: { $lte: 2 }
}).limit(10);
```

### Analyse RFM des Clients
```javascript
db.customers.aggregate([
  {
    $project: {
      customer_id: 1,
      city: "$customer_city",
      state: "$customer_state",
      recency: {
        $dateDiff: {
          startDate: "$profile.last_purchase_date",
          endDate: "$$NOW",
          unit: "day"
        }
      },
      frequency: "$profile.total_orders",
      monetary: "$profile.lifetime_value"
    }
  },
  {
    $addFields: {
      rfm_segment: {
        $cond: [
          { $and: [{ $lte: ["$recency", 90] }, { $gte: ["$frequency", 5] }] },
          "Champions",
          {
            $cond: [
              { $and: [{ $lte: ["$recency", 180] }, { $gte: ["$frequency", 3] }] },
              "Loyal",
              "At Risk"
            ]
          }
        ]
      }
    }
  },
  { $sort: { monetary: -1 } },
  { $limit: 100 }
]);
```

### Produits par Poids Moyen
```javascript
db.products.aggregate([
  { $group: { 
      _id: "$product_category_name", 
      avg_weight: { $avg: "$product_weight_g" },
      count: { $sum: 1 }
    } 
  },
  { $sort: { avg_weight: -1 } },
  { $limit: 15 }
]);
```

### Geospatial: Clients Proches
```javascript
db.customers.createIndex({ location: "2dsphere" });

db.customers.find({
  location: {
    $near: {
      $geometry: { type: "Point", coordinates: [-46.6333, -23.5505] }, // São Paulo
      $maxDistance: 50000 // 50km
    }
  }
}).limit(10);
```

---

## 🔵 CLICKHOUSE (SQL)

### KPIs Globaux
```sql
SELECT 
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as total_customers,
    COUNT(DISTINCT product_id) as total_products,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM sales_fact;
```

### Ventes Quotidiennes
```sql
SELECT 
    order_date,
    COUNT(*) as orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM sales_fact
WHERE order_date >= today() - INTERVAL 30 DAY
GROUP BY order_date
ORDER BY order_date DESC;
```

### Top 20 Produits
```sql
SELECT 
    product_id,
    product_category,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(price) as avg_price
FROM sales_fact
GROUP BY product_id, product_category
ORDER BY total_revenue DESC
LIMIT 20;
```

### Analyse par État
```sql
SELECT 
    customer_state,
    COUNT(DISTINCT customer_id) as customers,
    COUNT(*) as orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order,
    AVG(delivery_time_days) as avg_delivery_days
FROM sales_fact
WHERE delivery_time_days IS NOT NULL
GROUP BY customer_state
ORDER BY revenue DESC;
```

### Cohort Analysis
```sql
SELECT 
    toStartOfMonth(order_purchase_timestamp) as cohort_month,
    COUNT(DISTINCT customer_id) as customers,
    SUM(total_amount) as revenue
FROM sales_fact
GROUP BY cohort_month
ORDER BY cohort_month;
```

### Taux de Livraison à Temps
```sql
SELECT 
    customer_state,
    COUNT(*) as total_orders,
    SUM(CASE 
        WHEN order_delivered_timestamp <= order_estimated_delivery_date 
        THEN 1 ELSE 0 
    END) as on_time_deliveries,
    (on_time_deliveries / total_orders) * 100 as on_time_rate
FROM sales_fact
WHERE order_delivered_timestamp IS NOT NULL
GROUP BY customer_state
ORDER BY on_time_rate DESC;
```

### Segmentation ABC
```sql
WITH product_revenue AS (
    SELECT 
        product_id,
        SUM(total_amount) as revenue,
        COUNT(*) as orders
    FROM sales_fact
    GROUP BY product_id
),
cumulative AS (
    SELECT 
        product_id,
        revenue,
        orders,
        SUM(revenue) OVER (ORDER BY revenue DESC) as cumulative_revenue,
        SUM(revenue) OVER () as total_revenue
    FROM product_revenue
)
SELECT 
    product_id,
    revenue,
    orders,
    (cumulative_revenue / total_revenue) * 100 as cumulative_percentage,
    CASE 
        WHEN (cumulative_revenue / total_revenue) <= 0.8 THEN 'A'
        WHEN (cumulative_revenue / total_revenue) <= 0.95 THEN 'B'
        ELSE 'C'
    END as segment
FROM cumulative
ORDER BY revenue DESC;
```

---

## 🔴 NEO4J (Cypher)

### Recommandation Collaborative Filtering
```cypher
// Trouver des produits similaires basés sur les achats communs
MATCH (c:Customer)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(c2:Customer)
WHERE c.customer_id = 'specific_customer_id'
MATCH (c2)-[:PURCHASED]->(rec:Product)
WHERE NOT (c)-[:PURCHASED]->(rec)
RETURN rec.product_id, rec.category, COUNT(*) as score
ORDER BY score DESC
LIMIT 10;
```

### Market Basket Analysis
```cypher
// Produits fréquemment achetés ensemble
MATCH (p1:Product)<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(p2:Product)
WHERE p1.product_id < p2.product_id
RETURN 
    p1.product_id as product_1,
    p2.product_id as product_2,
    p1.category as category_1,
    p2.category as category_2,
    COUNT(*) as frequency
ORDER BY frequency DESC
LIMIT 20;
```

### Clients les Plus Influents
```cypher
// Top clients par nombre d'achats
MATCH (c:Customer)-[r:PURCHASED]->()
RETURN 
    c.customer_id,
    c.city,
    c.state,
    COUNT(r) as purchases,
    SUM(r.price) as total_spent
ORDER BY purchases DESC
LIMIT 20;
```

### Chemin Client → Produit via Vendeur
```cypher
MATCH path = (c:Customer)-[:PURCHASED]->(p:Product)<-[:SELLS]-(s:Seller)
WHERE c.customer_id = 'specific_customer_id'
RETURN path
LIMIT 10;
```

### Communautés de Clients (Louvain)
```cypher
// Créer une projection de graphe
CALL gds.graph.project(
    'customer-product-graph',
    ['Customer', 'Product'],
    {
        PURCHASED: {
            orientation: 'UNDIRECTED'
        }
    }
);

// Détecter les communautés
CALL gds.louvain.stream('customer-product-graph')
YIELD nodeId, communityId
WITH gds.util.asNode(nodeId) AS node, communityId
WHERE node:Customer
RETURN 
    communityId,
    COUNT(*) as community_size,
    COLLECT(node.customer_id)[0..5] as sample_customers
ORDER BY community_size DESC;
```

### PageRank des Produits
```cypher
// Produits les plus "centraux" dans le réseau
CALL gds.pageRank.stream('product-graph')
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS product, score
WHERE product:Product
RETURN 
    product.product_id,
    product.category,
    score
ORDER BY score DESC
LIMIT 20;
```

### Analyse de Similarité Produits
```cypher
// Produits similaires par comportement d'achat
MATCH (p:Product)<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(p2:Product)
WHERE p.product_id < p2.product_id
WITH p, p2, COUNT(DISTINCT c) as common_customers
WHERE common_customers > 5
RETURN 
    p.product_id,
    p2.product_id,
    p.category,
    p2.category,
    common_customers
ORDER BY common_customers DESC
LIMIT 20;
```

---

## 🔄 CROSS-DATABASE Queries (Trino)

### Jointure MongoDB + ClickHouse
```sql
-- Rejoindre les reviews MongoDB avec les ventes ClickHouse
SELECT 
    c.product_id,
    c.product_category,
    COUNT(DISTINCT c.order_id) as total_orders,
    SUM(c.total_amount) as revenue,
    AVG(m.review_score) as avg_rating,
    COUNT(m.review_id) as review_count
FROM clickhouse.ecommerce.sales_fact c
LEFT JOIN mongodb.ecommerce.reviews m ON c.order_id = m.order_id
GROUP BY c.product_id, c.product_category
ORDER BY revenue DESC
LIMIT 20;
```

### Analyse Complète Client
```sql
-- Combiner données de toutes les bases
SELECT 
    c.customer_id,
    c.customer_state,
    neo.purchase_count,
    ch.total_spent,
    ch.avg_order_value,
    mongo.review_count,
    mongo.avg_review_score
FROM clickhouse.ecommerce.customer_rfm c
JOIN neo4j.customer_stats neo ON c.customer_id = neo.customer_id
LEFT JOIN mongodb.ecommerce.customer_reviews mongo ON c.customer_id = mongo.customer_id
ORDER BY c.total_spent DESC
LIMIT 100;
```

---

## 📊 Business Intelligence

### Analyse AAARR (Pirate Metrics)

#### Acquisition
```sql
SELECT 
    toStartOfMonth(signup_date) as month,
    COUNT(*) as new_customers
FROM mongodb.ecommerce.customers
GROUP BY month;
```

#### Activation
```sql
SELECT 
    COUNT(DISTINCT customer_id) as customers_with_first_order,
    AVG(DATEDIFF('day', signup_date, first_order_date)) as avg_days_to_first_order
FROM customer_lifecycle;
```

#### Retention
```sql
SELECT 
    cohort_month,
    month_number,
    COUNT(DISTINCT customer_id) as active_customers,
    active_customers / first_month_size * 100 as retention_rate
FROM cohort_retention
GROUP BY cohort_month, month_number;
```

#### Revenue
```sql
SELECT 
    toYYYYMM(order_date) as month,
    SUM(total_amount) as revenue,
    COUNT(DISTINCT customer_id) as paying_customers,
    revenue / paying_customers as arpu
FROM sales_fact
GROUP BY month;
```

#### Referral
```sql
-- Analyse des patterns de recommandation via Neo4j
MATCH (c1:Customer)-[:RECOMMENDED]->(c2:Customer)
RETURN COUNT(*) as total_referrals;
```

---

## 🎯 Use Cases Avancés

### Détection d'Anomalies
```sql
WITH daily_stats AS (
    SELECT 
        order_date,
        COUNT(*) as orders,
        AVG(total_amount) as avg_amount,
        STDDEV(total_amount) as std_amount
    FROM sales_fact
    GROUP BY order_date
)
SELECT 
    order_date,
    orders,
    avg_amount,
    CASE 
        WHEN ABS(avg_amount - AVG(avg_amount) OVER ()) > 2 * STDDEV(avg_amount) OVER ()
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as status
FROM daily_stats
ORDER BY order_date DESC;
```

### Analyse de Churn
```sql
SELECT 
    customer_id,
    last_purchase_date,
    DATEDIFF('day', last_purchase_date, today()) as days_since_last_purchase,
    CASE 
        WHEN days_since_last_purchase > 180 THEN 'Churned'
        WHEN days_since_last_purchase > 90 THEN 'At Risk'
        WHEN days_since_last_purchase > 30 THEN 'Active'
        ELSE 'Very Active'
    END as status
FROM customer_rfm
ORDER BY days_since_last_purchase DESC;
```

---

**💡 Conseil**: Combinez ces requêtes avec les visualisations Streamlit pour créer des dashboards interactifs !
