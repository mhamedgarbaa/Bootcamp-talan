-- ========================================
-- CASSANDRA KEYSPACE & TABLES
-- ========================================

-- Création du Keyspace
CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE ecommerce;

-- Table pour les commandes (optimisée pour les requêtes par date)
CREATE TABLE IF NOT EXISTS orders_by_date (
    order_date date,
    order_hour int,
    order_id text,
    customer_id text,
    order_status text,
    purchase_timestamp timestamp,
    approved_at timestamp,
    delivered_at timestamp,
    estimated_delivery_date timestamp,
    total_amount decimal,
    payment_type text,
    PRIMARY KEY ((order_date), order_hour, order_id)
) WITH CLUSTERING ORDER BY (order_hour DESC, order_id ASC);

-- Table pour les requêtes par client
CREATE TABLE IF NOT EXISTS orders_by_customer (
    customer_id text,
    order_id text,
    purchase_timestamp timestamp,
    order_status text,
    total_amount decimal,
    PRIMARY KEY (customer_id, purchase_timestamp, order_id)
) WITH CLUSTERING ORDER BY (purchase_timestamp DESC);

-- Table pour analytics produits
CREATE TABLE IF NOT EXISTS product_sales (
    product_id text,
    year_month text,
    order_id text,
    purchase_timestamp timestamp,
    quantity int,
    price decimal,
    seller_id text,
    PRIMARY KEY ((product_id, year_month), purchase_timestamp, order_id)
) WITH CLUSTERING ORDER BY (purchase_timestamp DESC);

-- Index secondaires
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders_by_date(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_payment ON orders_by_date(payment_type);
