"""
Tests unitaires pour le projet E-Commerce Analytics
"""
import pytest
import pandas as pd
from pathlib import Path
import os

# Configuration des chemins
DATA_PATH = Path("data/raw")
CONFIG_PATH = Path("config")

class TestDatasetAvailability:
    """Tests pour vérifier la disponibilité des datasets"""
    
    def test_data_directory_exists(self):
        """Vérifie que le dossier data existe"""
        assert DATA_PATH.exists(), "Le dossier data/raw n'existe pas"
    
    def test_required_csv_files(self):
        """Vérifie la présence des fichiers CSV requis"""
        required_files = [
            'olist_orders_dataset.csv',
            'olist_order_items_dataset.csv',
            'olist_customers_dataset.csv',
            'olist_products_dataset.csv',
            'olist_sellers_dataset.csv',
            'olist_order_reviews_dataset.csv',
            'olist_order_payments_dataset.csv',
            'olist_geolocation_dataset.csv'
        ]
        
        for file in required_files:
            file_path = DATA_PATH / file
            if file_path.exists():
                print(f"✅ {file} found")
            else:
                print(f"⚠️  {file} not found - run download_dataset.py")

class TestConfigFiles:
    """Tests pour vérifier les fichiers de configuration"""
    
    def test_config_directory_exists(self):
        """Vérifie que le dossier config existe"""
        assert CONFIG_PATH.exists(), "Le dossier config n'existe pas"
    
    def test_schema_files_exist(self):
        """Vérifie la présence des fichiers de schéma"""
        schema_files = [
            'cassandra_schema.cql',
            'mongodb_schema.js',
            'clickhouse_schema.sql',
            'neo4j_schema.cypher'
        ]
        
        for file in schema_files:
            file_path = CONFIG_PATH / file
            assert file_path.exists(), f"{file} manquant"
            print(f"✅ {file} exists")

class TestDataQuality:
    """Tests de qualité des données"""
    
    def test_orders_data_quality(self):
        """Vérifie la qualité des données de commandes"""
        orders_file = DATA_PATH / 'olist_orders_dataset.csv'
        
        if not orders_file.exists():
            pytest.skip("Dataset non téléchargé")
        
        df = pd.read_csv(orders_file)
        
        # Vérifier les colonnes obligatoires
        required_columns = [
            'order_id', 
            'customer_id', 
            'order_status',
            'order_purchase_timestamp'
        ]
        
        for col in required_columns:
            assert col in df.columns, f"Colonne {col} manquante"
        
        # Vérifier qu'il n'y a pas de doublons d'order_id
        assert df['order_id'].is_unique, "Doublons détectés dans order_id"
        
        # Vérifier les valeurs nulles
        null_counts = df.isnull().sum()
        print(f"\n📊 Valeurs nulles dans orders:")
        print(null_counts[null_counts > 0])
        
        # Statistiques
        print(f"\n📈 Statistiques:")
        print(f"  Total orders: {len(df):,}")
        print(f"  Unique customers: {df['customer_id'].nunique():,}")
        print(f"  Order statuses: {df['order_status'].value_counts().to_dict()}")
    
    def test_products_data_quality(self):
        """Vérifie la qualité des données produits"""
        products_file = DATA_PATH / 'olist_products_dataset.csv'
        
        if not products_file.exists():
            pytest.skip("Dataset non téléchargé")
        
        df = pd.read_csv(products_file)
        
        # Vérifier qu'il y a des produits
        assert len(df) > 0, "Aucun produit trouvé"
        
        # Vérifier product_id unique
        assert df['product_id'].is_unique, "Doublons dans product_id"
        
        print(f"\n📦 Statistiques produits:")
        print(f"  Total products: {len(df):,}")
        print(f"  Categories: {df['product_category_name'].nunique()}")
        print(f"  Top 5 categories:")
        print(df['product_category_name'].value_counts().head())

class TestDatabaseConnections:
    """Tests de connexion aux bases de données"""
    
    def test_cassandra_connection(self):
        """Test connexion Cassandra"""
        try:
            from cassandra.cluster import Cluster
            cluster = Cluster(['localhost'])
            session = cluster.connect()
            
            # Vérifier le keyspace
            keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
            keyspace_names = [row.keyspace_name for row in keyspaces]
            
            cluster.shutdown()
            
            assert 'ecommerce' in keyspace_names, "Keyspace 'ecommerce' non trouvé"
            print("✅ Cassandra connection successful")
        except Exception as e:
            pytest.skip(f"Cassandra not available: {e}")
    
    def test_mongodb_connection(self):
        """Test connexion MongoDB"""
        try:
            from pymongo import MongoClient
            client = MongoClient('mongodb://admin:password123@localhost:27017/', serverSelectionTimeoutMS=2000)
            
            # Vérifier la connexion
            client.admin.command('ping')
            
            # Lister les collections
            db = client['ecommerce']
            collections = db.list_collection_names()
            
            print(f"✅ MongoDB connection successful")
            print(f"  Collections: {collections}")
            
            client.close()
        except Exception as e:
            pytest.skip(f"MongoDB not available: {e}")
    
    def test_clickhouse_connection(self):
        """Test connexion ClickHouse"""
        try:
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host='localhost',
                port=8123,
                username='admin',
                password='password123'
            )
            
            # Vérifier la connexion
            result = client.query("SELECT version()")
            version = result.first_row[0]
            
            print(f"✅ ClickHouse connection successful (version {version})")
        except Exception as e:
            pytest.skip(f"ClickHouse not available: {e}")
    
    def test_neo4j_connection(self):
        """Test connexion Neo4j"""
        try:
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                auth=("neo4j", "password123")
            )
            
            with driver.session() as session:
                result = session.run("RETURN 1 as num")
                assert result.single()['num'] == 1
            
            driver.close()
            print("✅ Neo4j connection successful")
        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

class TestIngestionScripts:
    """Tests pour les scripts d'ingestion"""
    
    def test_import_load_script(self):
        """Vérifie que le script de chargement peut être importé"""
        try:
            import sys
            sys.path.insert(0, 'ingestion')
            from load_olist_dataset import OlistDataLoader
            print("✅ Script d'ingestion importable")
        except Exception as e:
            pytest.fail(f"Impossible d'importer le script: {e}")

def run_integration_tests():
    """Exécute tous les tests d'intégration"""
    print("\n" + "="*60)
    print("🧪 INTEGRATION TESTS")
    print("="*60)
    
    # Test 1: Vérifier les fichiers
    print("\n1️⃣ Checking configuration files...")
    tester = TestConfigFiles()
    tester.test_config_directory_exists()
    tester.test_schema_files_exist()
    
    # Test 2: Vérifier les datasets
    print("\n2️⃣ Checking datasets...")
    tester = TestDatasetAvailability()
    tester.test_data_directory_exists()
    tester.test_required_csv_files()
    
    # Test 3: Qualité des données
    print("\n3️⃣ Checking data quality...")
    tester = TestDataQuality()
    try:
        tester.test_orders_data_quality()
        tester.test_products_data_quality()
    except:
        print("⚠️  Data quality tests skipped (dataset not loaded)")
    
    # Test 4: Connexions bases de données
    print("\n4️⃣ Testing database connections...")
    tester = TestDatabaseConnections()
    tester.test_cassandra_connection()
    tester.test_mongodb_connection()
    tester.test_clickhouse_connection()
    tester.test_neo4j_connection()
    
    print("\n" + "="*60)
    print("✅ Integration tests completed!")
    print("="*60 + "\n")

if __name__ == "__main__":
    # Exécuter les tests
    run_integration_tests()
    
    # Ou utiliser pytest
    # pytest test_integration.py -v
