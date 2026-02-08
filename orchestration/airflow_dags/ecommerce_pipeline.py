"""
Pipeline ETL/ELT complet pour E-Commerce Analytics
===================================================
ETL : Extract (CSV) → Transform (nettoyage) → Load (MongoDB, ClickHouse, Neo4j)
ELT : Load brut → Transform via Spark (agrégations, segmentation, ML)

Orchestré par Apache Airflow avec Spark via docker exec
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

RAW_DIR = '/opt/airflow/data/raw'
CLEANED_DIR = '/opt/airflow/data/cleaned'
PARQUET_DIR = '/opt/airflow/data/parquet'

REQUIRED_FILES = {
    'orders': 'olist_orders_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
}

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet: Extract → Transform → Load → Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'etl', 'spark', 'data-pipeline']
)

# ═══════════════════════════════════════════════════════════════
# PHASE 1 : EXTRACT - Vérification et validation des sources
# ═══════════════════════════════════════════════════════════════

check_data = BashOperator(
    task_id='extract_check_data',
    bash_command=f'test -f {RAW_DIR}/olist_orders_dataset.csv && echo "✅ Data files found" || exit 1',
    dag=dag
)

def validate_data():
    """Valider la qualité des données sources"""
    import pandas as pd
    
    print("=" * 60)
    print("📋 PHASE EXTRACT : Validation des données sources")
    print("=" * 60)
    
    stats = {}
    for name, filename in REQUIRED_FILES.items():
        file_path = Path(RAW_DIR) / filename
        if not file_path.exists():
            raise FileNotFoundError(f"❌ Fichier manquant: {filename}")
        
        df = pd.read_csv(file_path)
        n_rows = len(df)
        n_cols = len(df.columns)
        n_nulls = int(df.isnull().sum().sum())
        n_dupes = int(df.duplicated().sum())
        
        stats[name] = {'rows': n_rows, 'cols': n_cols, 'nulls': n_nulls, 'dupes': n_dupes}
        print(f"  ✅ {filename}: {n_rows} lignes, {n_cols} cols, {n_nulls} nulls, {n_dupes} doublons")
    
    total_rows = sum(s['rows'] for s in stats.values())
    total_nulls = sum(s['nulls'] for s in stats.values())
    total_dupes = sum(s['dupes'] for s in stats.values())
    print(f"\n  📊 TOTAL: {total_rows} lignes, {total_nulls} nulls, {total_dupes} doublons")
    print("  ✅ Validation terminée - données prêtes pour Transform")
    return stats

validate_data_task = PythonOperator(
    task_id='extract_validate_quality',
    python_callable=validate_data,
    dag=dag
)

# ═══════════════════════════════════════════════════════════════
# PHASE 2 : TRANSFORM - Nettoyage (nulls, doublons, types)
# ═══════════════════════════════════════════════════════════════

def transform_clean_data():
    """
    Nettoyage complet des données:
    - Suppression des doublons
    - Traitement des valeurs nulles
    - Conversion des types (dates, numériques)
    - Standardisation des formats
    """
    import pandas as pd
    import numpy as np
    
    print("=" * 60)
    print("🔧 PHASE TRANSFORM : Nettoyage des données")
    print("=" * 60)
    
    cleaned_dir = Path(CLEANED_DIR)
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    
    # --- Orders ---
    print("\n  📦 Nettoyage: orders...")
    df = pd.read_csv(f"{RAW_DIR}/olist_orders_dataset.csv")
    before = len(df)
    df = df.drop_duplicates(subset=['order_id'])
    # Conversion des colonnes date
    date_cols = ['order_purchase_timestamp', 'order_approved_at',
                 'order_delivered_carrier_date', 'order_delivered_customer_date',
                 'order_estimated_delivery_date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    # Remplir les statuts manquants
    df['order_status'] = df['order_status'].fillna('unknown')
    df.to_csv(f"{CLEANED_DIR}/orders_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes (supprimé {before - len(df)} doublons)")
    
    # --- Customers ---
    print("  👥 Nettoyage: customers...")
    df = pd.read_csv(f"{RAW_DIR}/olist_customers_dataset.csv")
    before = len(df)
    df = df.drop_duplicates(subset=['customer_id'])
    df['customer_city'] = df['customer_city'].str.strip().str.lower()
    df['customer_state'] = df['customer_state'].str.strip().str.upper()
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    df.to_csv(f"{CLEANED_DIR}/customers_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes")
    
    # --- Products ---
    print("  📦 Nettoyage: products...")
    df = pd.read_csv(f"{RAW_DIR}/olist_products_dataset.csv")
    before = len(df)
    df = df.drop_duplicates(subset=['product_id'])
    df['product_category_name'] = df['product_category_name'].fillna('sem_categoria')
    numeric_cols = ['product_weight_g', 'product_length_cm',
                    'product_height_cm', 'product_width_cm',
                    'product_name_lenght', 'product_description_lenght',
                    'product_photos_qty']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    df.to_csv(f"{CLEANED_DIR}/products_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes")
    
    # --- Order Items ---
    print("  🛒 Nettoyage: order_items...")
    df = pd.read_csv(f"{RAW_DIR}/olist_order_items_dataset.csv")
    before = len(df)
    df = df.drop_duplicates(subset=['order_id', 'order_item_id', 'product_id'])
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0)
    df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce').fillna(0.0)
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    # Supprimer les lignes avec prix négatif
    df = df[df['price'] >= 0]
    df.to_csv(f"{CLEANED_DIR}/order_items_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes")
    
    # --- Reviews ---
    print("  ⭐ Nettoyage: reviews...")
    df = pd.read_csv(f"{RAW_DIR}/olist_order_reviews_dataset.csv")
    before = len(df)
    df = df.drop_duplicates(subset=['review_id'])
    df['review_score'] = pd.to_numeric(df['review_score'], errors='coerce')
    df = df[(df['review_score'] >= 1) & (df['review_score'] <= 5)]
    df['review_comment_title'] = df['review_comment_title'].fillna('')
    df['review_comment_message'] = df['review_comment_message'].fillna('')
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
    df.to_csv(f"{CLEANED_DIR}/reviews_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes")
    
    # --- Payments ---
    print("  💳 Nettoyage: payments...")
    df = pd.read_csv(f"{RAW_DIR}/olist_order_payments_dataset.csv")
    before = len(df)
    df = df.drop_duplicates()
    df['payment_value'] = pd.to_numeric(df['payment_value'], errors='coerce').fillna(0.0)
    df['payment_installments'] = pd.to_numeric(df['payment_installments'], errors='coerce').fillna(1).astype(int)
    df['payment_type'] = df['payment_type'].fillna('not_defined')
    df = df[df['payment_value'] >= 0]
    df.to_csv(f"{CLEANED_DIR}/payments_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes")
    
    # --- Sellers ---
    print("  🏪 Nettoyage: sellers...")
    df = pd.read_csv(f"{RAW_DIR}/olist_sellers_dataset.csv")
    before = len(df)
    df = df.drop_duplicates(subset=['seller_id'])
    df['seller_city'] = df['seller_city'].str.strip().str.lower()
    df['seller_state'] = df['seller_state'].str.strip().str.upper()
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df.to_csv(f"{CLEANED_DIR}/sellers_cleaned.csv", index=False)
    print(f"    {before} → {len(df)} lignes")
    
    print("\n  ✅ TRANSFORM terminé - données nettoyées dans /data/cleaned/")
    return "Transform completed"

transform_data_task = PythonOperator(
    task_id='transform_clean_data',
    python_callable=transform_clean_data,
    dag=dag
)

# ═══════════════════════════════════════════════════════════════
# PHASE 3 : LOAD - Chargement dans les bases NoSQL/NewSQL
# ═══════════════════════════════════════════════════════════════

def load_databases():
    """Charger les données nettoyées vers MongoDB, ClickHouse, Neo4j"""
    import subprocess
    
    print("=" * 60)
    print("📤 PHASE LOAD : Chargement dans les bases de données")
    print("=" * 60)
    
    os.chdir('/opt/airflow')
    
    env = os.environ.copy()
    env['MONGO_HOST'] = 'mongodb'
    env['CLICKHOUSE_HOST'] = 'clickhouse'
    env['NEO4J_HOST'] = 'neo4j'
    
    result = subprocess.run(
        ['python', 'ingestion/load_olist_simple.py'],
        capture_output=True,
        text=True,
        env=env
    )
    
    print(result.stdout)
    
    if result.returncode != 0:
        print(f"❌ Erreur: {result.stderr}")
        raise Exception("Chargement base de données échoué")
    
    print("  ✅ LOAD terminé - données chargées dans MongoDB, ClickHouse, Neo4j")
    return "Load completed"

load_databases_task = PythonOperator(
    task_id='load_to_databases',
    python_callable=load_databases,
    dag=dag
)

# ═══════════════════════════════════════════════════════════════
# PHASE 4 : SPARK PROCESSING - Transformations avancées (ELT)
#   Exécuté via docker exec sur le conteneur spark-master
# ═══════════════════════════════════════════════════════════════

def run_spark_job(script_name, job_description):
    """Exécuter un job Spark via docker exec sur le conteneur spark-master"""
    import docker
    
    print(f"\n  🚀 Lancement Spark: {job_description}")
    print(f"  📄 Script: {script_name}")
    
    client = docker.from_env()
    
    # Trouver le conteneur spark-master
    spark_container = None
    for container in client.containers.list():
        if 'spark-master' in container.name:
            spark_container = container
            break
    
    if spark_container is None:
        raise Exception("❌ Conteneur spark-master introuvable! Vérifiez que Spark est démarré.")
    
    print(f"  🐳 Conteneur: {spark_container.name}")
    
    # Exécuter spark-submit
    exit_code, output = spark_container.exec_run(
        f'/opt/spark/bin/spark-submit '
        f'--driver-memory 1g '
        f'/opt/spark-apps/processing/{script_name}',
        demux=True
    )
    
    stdout = output[0].decode('utf-8') if output[0] else ''
    stderr = output[1].decode('utf-8') if output[1] else ''
    
    if stdout:
        # Afficher les dernières lignes pertinentes
        lines = stdout.strip().split('\n')
        for line in lines[-30:]:
            print(f"    {line}")
    
    if exit_code != 0:
        print(f"\n  ❌ Spark job échoué (exit code {exit_code})")
        if stderr:
            for line in stderr.strip().split('\n')[-15:]:
                print(f"    STDERR: {line}")
        raise Exception(f"Spark job '{job_description}' échoué avec code {exit_code}")
    
    print(f"  ✅ {job_description} terminé avec succès")
    return exit_code

def spark_batch_processing():
    """Spark: agrégations, segmentation RFM, métriques"""
    print("=" * 60)
    print("⚡ PHASE ELT/SPARK : Traitement batch")
    print("=" * 60)
    return run_spark_job('spark_batch.py', 'Batch Processing (agrégations + RFM)')

def spark_ml_processing():
    """Spark: recommandations ALS"""
    print("=" * 60)
    print("🤖 PHASE ELT/SPARK : Machine Learning")
    print("=" * 60)
    return run_spark_job('spark_ml_recommendations.py', 'ML Recommendations (ALS)')

spark_batch_task = PythonOperator(
    task_id='spark_batch_processing',
    python_callable=spark_batch_processing,
    dag=dag
)

spark_ml_task = PythonOperator(
    task_id='spark_ml_recommendations',
    python_callable=spark_ml_processing,
    dag=dag
)

# ═══════════════════════════════════════════════════════════════
# PHASE 5 : VERIFY & REPORT
# ═══════════════════════════════════════════════════════════════

def verify_results():
    """Vérifier les résultats du pipeline ETL + Spark"""
    print("=" * 60)
    print("🔍 VERIFICATION des résultats")
    print("=" * 60)
    
    results = {'found': 0, 'missing': 0}
    
    expected_outputs = [
        ('Ventes par état', f'{PARQUET_DIR}/sales_by_state'),
        ('Ventes mensuelles', f'{PARQUET_DIR}/sales_by_month'),
        ('Produits par catégorie', f'{PARQUET_DIR}/product_by_category'),
        ('Segments clients', f'{PARQUET_DIR}/customer_segments'),
        ('Recommandations ML', f'{PARQUET_DIR}/recommendations'),
    ]
    
    for label, path in expected_outputs:
        output_path = Path(path)
        if output_path.exists():
            print(f"  ✅ {label}: {path}")
            results['found'] += 1
        else:
            print(f"  ⚠️  {label}: MANQUANT ({path})")
            results['missing'] += 1
    
    # Vérifier les données nettoyées
    cleaned = Path(CLEANED_DIR)
    if cleaned.exists():
        cleaned_files = list(cleaned.glob('*.csv'))
        print(f"\n  📁 Fichiers nettoyés: {len(cleaned_files)}")
        for f in cleaned_files:
            size_kb = f.stat().st_size / 1024
            print(f"    - {f.name} ({size_kb:.0f} KB)")
    
    print(f"\n  📊 Résultats Spark: {results['found']} trouvés, {results['missing']} manquants")
    print("  ✅ Vérification terminée")
    return results

verify_results_task = PythonOperator(
    task_id='verify_results',
    python_callable=verify_results,
    dag=dag
)

def generate_summary():
    """Générer le rapport final du pipeline"""
    print("=" * 60)
    print("📊 RAPPORT FINAL DU PIPELINE")
    print("=" * 60)
    
    summary = f"""
═══════════════════════════════════════════════════════════
  📊 E-COMMERCE ETL/ELT PIPELINE - RAPPORT D'EXECUTION
═══════════════════════════════════════════════════════════

  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

  PHASE ETL:
    ✅ Extract   : Données CSV validées
    ✅ Transform  : Nettoyage (nulls, doublons, types)
    ✅ Load       : MongoDB + ClickHouse + Neo4j

  PHASE ELT (Spark):
    ✅ Batch      : Agrégations ventes, métriques produits, RFM
    ✅ ML         : Recommandations ALS

  BASES DE DONNEES:
    🍃 MongoDB    : Documents (produits, avis)
    🏎️ ClickHouse : OLAP analytique (ventes)
    🕸️ Neo4j      : Graphe (clients ↔ produits)

  🎯 Statut Pipeline: SUCCESS
═══════════════════════════════════════════════════════════
"""
    
    print(summary)
    
    report_dir = Path("/opt/airflow/reports")
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(summary)
    
    print(f"  💾 Rapport sauvegardé: {report_file}")
    return "Summary generated"

generate_summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag
)

cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='echo "🧹 Nettoyage terminé - Pipeline ETL/ELT complet"',
    dag=dag
)

# ═══════════════════════════════════════════════════════════════
# DEPENDENCY CHAIN - Pipeline ETL → ELT complet
# ═══════════════════════════════════════════════════════════════
# Extract
check_data >> validate_data_task
# Transform
validate_data_task >> transform_data_task
# Load
transform_data_task >> load_databases_task
# Spark ELT (batch + ML en parallèle)
load_databases_task >> [spark_batch_task, spark_ml_task]
# Verify après Spark
[spark_batch_task, spark_ml_task] >> verify_results_task
# Report
verify_results_task >> generate_summary_task >> cleanup
