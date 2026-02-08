"""
Chargement simplifié du dataset Olist (sans Cassandra)
"""
import pandas as pd
import numpy as np
from pymongo import MongoClient
import clickhouse_connect
from neo4j import GraphDatabase
from datetime import datetime
import logging
from pathlib import Path
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OlistDataLoaderSimple:
    def __init__(self, data_path="data/raw/", 
                 mongo_host=None, clickhouse_host=None, neo4j_host=None):
        self.data_path = data_path
        
        # Utiliser les variables d'environnement ou les valeurs par défaut
        mongo_host = mongo_host or os.getenv('MONGO_HOST', 'localhost')
        clickhouse_host = clickhouse_host or os.getenv('CLICKHOUSE_HOST', 'localhost')
        neo4j_host = neo4j_host or os.getenv('NEO4J_HOST', 'localhost')
        
        # Connexions
        logger.info("🔗 Connecting to databases...")
        
        try:
            self.mongo_client = MongoClient(
                f'mongodb://admin:password123@{mongo_host}:27017/', 
                serverSelectionTimeoutMS=5000
            )
            self.mongo_db = self.mongo_client['ecommerce']
            logger.info(f"✅ Connected to MongoDB at {mongo_host}")
        except Exception as e:
            logger.warning(f"⚠️  MongoDB connection failed: {e}")
            self.mongo_db = None
        
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=clickhouse_host,
                port=8123,
                username='admin',
                password='password123',
                database='ecommerce',
                connect_timeout=10
            )
            logger.info(f"✅ Connected to ClickHouse at {clickhouse_host}")
        except Exception as e:
            logger.warning(f"⚠️  ClickHouse connection failed: {e}")
            self.clickhouse_client = None
        
        try:
            self.neo4j_driver = GraphDatabase.driver(
                f"bolt://{neo4j_host}:7687",
                auth=("neo4j", "password123"),
                connection_timeout=10
            )
            logger.info(f"✅ Connected to Neo4j at {neo4j_host}")
        except Exception as e:
            logger.warning(f"⚠️  Neo4j connection failed: {e}")
            self.neo4j_driver = None
    
    def load_csv_files(self):
        """Chargement des CSV en DataFrames"""
        logger.info("📥 Loading CSV files...")
        
        try:
            self.df_orders = pd.read_csv(f"{self.data_path}olist_orders_dataset.csv")
            self.df_order_items = pd.read_csv(f"{self.data_path}olist_order_items_dataset.csv")
            self.df_order_payments = pd.read_csv(f"{self.data_path}olist_order_payments_dataset.csv")
            self.df_customers = pd.read_csv(f"{self.data_path}olist_customers_dataset.csv")
            self.df_products = pd.read_csv(f"{self.data_path}olist_products_dataset.csv")
            self.df_sellers = pd.read_csv(f"{self.data_path}olist_sellers_dataset.csv")
            self.df_reviews = pd.read_csv(f"{self.data_path}olist_order_reviews_dataset.csv")
            
            logger.info(f"✅ Loaded {len(self.df_orders)} orders")
            logger.info(f"✅ Loaded {len(self.df_customers)} customers")
            logger.info(f"✅ Loaded {len(self.df_products)} products")
        except Exception as e:
            logger.error(f"❌ Error loading CSV files: {e}")
            raise
    
    def load_to_mongodb(self):
        """Chargement vers MongoDB (Documents)"""
        if self.mongo_db is None:
            logger.warning("⏭️  Skipping MongoDB load (not connected)")
            return
            
        logger.info("📤 Loading data to MongoDB...")
        
        try:
            # Products collection (limité à 1000 pour être rapide)
            products_data = self.df_products.head(1000).to_dict('records')
            for product in products_data:
                product['_id'] = product['product_id']
                product['created_at'] = datetime.utcnow()
                product['updated_at'] = datetime.utcnow()
            
            self.mongo_db.products.delete_many({})
            if products_data:
                self.mongo_db.products.insert_many(products_data, ordered=False)
            logger.info(f"✅ Loaded {len(products_data)} products to MongoDB")
            
            # Reviews (limité à 1000)
            reviews_data = []
            for _, row in self.df_reviews.head(1000).iterrows():
                review_doc = {
                    '_id': row['review_id'],
                    'review_id': row['review_id'],
                    'order_id': row['order_id'],
                    'review_score': int(row['review_score']),
                    'review_comment_title': row['review_comment_title'] if pd.notna(row['review_comment_title']) else None,
                    'review_comment_message': row['review_comment_message'] if pd.notna(row['review_comment_message']) else None,
                    'review_creation_date': pd.to_datetime(row['review_creation_date']),
                }
                reviews_data.append(review_doc)
            
            self.mongo_db.reviews.delete_many({})
            if reviews_data:
                self.mongo_db.reviews.insert_many(reviews_data, ordered=False)
            logger.info(f"✅ Loaded {len(reviews_data)} reviews to MongoDB")
            
        except Exception as e:
            logger.error(f"❌ Error loading to MongoDB: {e}")
    
    def load_to_clickhouse(self):
        """Chargement vers ClickHouse (OLAP)"""
        if self.clickhouse_client is None:
            logger.warning("⏭️  Skipping ClickHouse load (not connected)")
            return
            
        logger.info("📤 Loading data to ClickHouse...")
        
        try:
            # Créer la base de données
            self.clickhouse_client.command("CREATE DATABASE IF NOT EXISTS ecommerce")
            
            # Créer la table simplifiée
            self.clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS ecommerce.sales_fact (
                    order_id String,
                    customer_id String,
                    product_id String,
                    order_purchase_timestamp DateTime,
                    order_status String,
                    price Float64,
                    freight_value Float64
                )
                ENGINE = MergeTree()
                ORDER BY (order_purchase_timestamp, order_id)
            """)
            
            # Préparer les données (limité à 1000 lignes)
            df_sales = self.df_orders.head(1000).merge(
                self.df_order_items, 
                on='order_id', 
                how='left'
            )[[
                'order_id', 'customer_id', 'product_id',
                'order_purchase_timestamp', 'order_status', 
                'price', 'freight_value'
            ]]
            
            # Conversion des dates
            df_sales['order_purchase_timestamp'] = pd.to_datetime(df_sales['order_purchase_timestamp'], errors='coerce')
            df_sales = df_sales.fillna({'price': 0, 'freight_value': 0, 'product_id': 'unknown'})
            
            # Insert
            self.clickhouse_client.insert_df('ecommerce.sales_fact', df_sales)
            logger.info(f"✅ Loaded {len(df_sales)} sales records to ClickHouse")
            
        except Exception as e:
            logger.error(f"❌ Error loading to ClickHouse: {e}")
    
    def load_to_neo4j(self):
        """Chargement vers Neo4j (Graph) - Version simplifiée"""
        if self.neo4j_driver is None:
            logger.warning("⏭️  Skipping Neo4j load (not connected)")
            return
            
        logger.info("📤 Loading data to Neo4j...")
        
        try:
            with self.neo4j_driver.session() as session:
                # Clear existing data
                logger.info("  Clearing existing data...")
                session.run("MATCH (n) DETACH DELETE n")
                
                # Créer les contraintes
                session.run("CREATE CONSTRAINT customer_id_unique IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE")
                session.run("CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE")
                
                # Stratégie: partir des orders et prendre les entités liées
                logger.info("  Selecting orders and related data...")
                
                # Prendre 100 orders avec leurs items
                sample_orders = self.df_orders.head(100)
                df_orders_products = sample_orders.merge(
                    self.df_order_items, 
                    on='order_id', 
                    how='inner'
                )
                
                # Extraire les customer_ids et product_ids uniques
                customer_ids = df_orders_products['customer_id'].unique()[:100]
                product_ids = df_orders_products['product_id'].unique()[:100]
                
                # Filtrer customers et products correspondants
                df_customers_subset = self.df_customers[self.df_customers['customer_id'].isin(customer_ids)]
                df_products_subset = self.df_products[self.df_products['product_id'].isin(product_ids)]
                
                # Créer les nœuds Customer
                logger.info(f"  Creating {len(df_customers_subset)} Customer nodes...")
                for idx, row in df_customers_subset.iterrows():
                    session.run("""
                        MERGE (c:Customer {customer_id: $customer_id})
                        SET c.city = $city, c.state = $state
                    """, customer_id=row['customer_id'],
                         city=row['customer_city'],
                         state=row['customer_state'])
                
                logger.info(f"  ✅ Created {len(df_customers_subset)} Customer nodes")
                
                # Créer les nœuds Product
                logger.info(f"  Creating {len(df_products_subset)} Product nodes...")
                for idx, row in df_products_subset.iterrows():
                    session.run("""
                        MERGE (p:Product {product_id: $product_id})
                        SET p.category = $category
                    """, product_id=row['product_id'],
                         category=row['product_category_name'] if pd.notna(row['product_category_name']) else 'unknown')
                
                logger.info(f"  ✅ Created {len(df_products_subset)} Product nodes")
                
                # Créer les relations PURCHASED
                logger.info("  Creating relationships...")
                
                # Filtrer pour ne garder que les relations valides
                df_valid_relations = df_orders_products[
                    df_orders_products['customer_id'].isin(customer_ids) &
                    df_orders_products['product_id'].isin(product_ids)
                ].head(200)
                
                logger.info(f"  Found {len(df_valid_relations)} valid relationships to create")
                
                relationships_created = 0
                for idx, row in df_valid_relations.iterrows():
                    try:
                        result = session.run("""
                            MATCH (c:Customer {customer_id: $customer_id})
                            MATCH (p:Product {product_id: $product_id})
                            MERGE (c)-[r:PURCHASED {order_id: $order_id, product_id: $product_id}]->(p)
                            SET r.price = $price, r.freight_value = $freight_value
                            RETURN r
                        """, customer_id=row['customer_id'],
                             product_id=row['product_id'],
                             order_id=row['order_id'],
                             price=float(row['price']) if pd.notna(row['price']) else 0.0,
                             freight_value=float(row['freight_value']) if pd.notna(row['freight_value']) else 0.0)
                        
                        if result.single():
                            relationships_created += 1
                    except Exception as e:
                        logger.debug(f"  Skipped relationship: {e}")
                
                logger.info(f"  ✅ Created {relationships_created} relationships")
            
            logger.info("✅ Graph loaded to Neo4j")
        except Exception as e:
            logger.error(f"❌ Error loading to Neo4j: {e}")
    
    def run_full_load(self):
        """Exécution complète du chargement"""
        logger.info("🚀 Starting data load (simplified version)...")
        
        self.load_csv_files()
        
        logger.info("\n1️⃣ Loading to MongoDB (Documents)")
        self.load_to_mongodb()
        
        logger.info("\n2️⃣ Loading to ClickHouse (OLAP)")
        self.load_to_clickhouse()
        
        logger.info("\n3️⃣ Loading to Neo4j (Graph)")
        self.load_to_neo4j()
        
        logger.info("\n✅ Data load completed!")
        
        # Fermeture des connexions
        if self.mongo_db is not None:
            self.mongo_client.close()
        if self.neo4j_driver is not None:
            self.neo4j_driver.close()

if __name__ == "__main__":
    loader = OlistDataLoaderSimple(data_path="data/raw/")
    loader.run_full_load()
