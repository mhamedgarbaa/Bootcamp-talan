"""
Script Spark pour traitement batch des données Olist
Agrégations, transformations et préparation pour ML
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session(app_name="Olist E-commerce Analytics"):
    """Créer une session Spark"""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()

def load_csv_data(spark, data_path="/opt/spark-apps/data/raw/"):
    """Charger tous les CSV en DataFrames Spark"""
    print("📥 Loading CSV files...")
    
    df_orders = spark.read.csv(
        f"{data_path}olist_orders_dataset.csv",
        header=True,
        inferSchema=True
    )
    
    df_customers = spark.read.csv(
        f"{data_path}olist_customers_dataset.csv",
        header=True,
        inferSchema=True
    )
    
    df_products = spark.read.csv(
        f"{data_path}olist_products_dataset.csv",
        header=True,
        inferSchema=True
    )
    
    df_order_items = spark.read.csv(
        f"{data_path}olist_order_items_dataset.csv",
        header=True,
        inferSchema=True
    )
    
    df_reviews = spark.read.csv(
        f"{data_path}olist_order_reviews_dataset.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ Loaded orders: {df_orders.count()} rows")
    print(f"✅ Loaded customers: {df_customers.count()} rows")
    print(f"✅ Loaded products: {df_products.count()} rows")
    
    return df_orders, df_customers, df_products, df_order_items, df_reviews

def compute_sales_aggregations(spark, df_orders, df_order_items, df_customers):
    """Calculer les agrégations de ventes"""
    print("\n📊 Computing sales aggregations...")
    
    # Joindre orders + order_items + customers
    df_sales = df_orders \
        .join(df_order_items, "order_id") \
        .join(df_customers, "customer_id")
    
    # Agrégation par état
    df_by_state = df_sales.groupBy("customer_state") \
        .agg(
            count("order_id").alias("total_orders"),
            sum(col("price") + col("freight_value")).alias("total_revenue"),
            avg(col("price") + col("freight_value")).alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("total_revenue"))
    
    print("\n🏆 Top 10 States by Revenue:")
    df_by_state.show(10)
    
    # Agrégation temporelle (mensuelle)
    df_by_month = df_sales \
        .withColumn("order_month", date_format("order_purchase_timestamp", "yyyy-MM")) \
        .groupBy("order_month") \
        .agg(
            count("order_id").alias("total_orders"),
            sum(col("price") + col("freight_value")).alias("total_revenue"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("order_month")
    
    print("\n📅 Monthly Sales Trend:")
    df_by_month.show()
    
    return df_by_state, df_by_month

def compute_product_metrics(spark, df_products, df_order_items, df_reviews):
    """Calculer les métriques produits"""
    print("\n📦 Computing product metrics...")
    
    # Joindre produits + items + reviews
    df_product_stats = df_order_items \
        .join(df_products, "product_id") \
        .join(df_reviews, "order_id", "left")
    
    # Agrégations par catégorie
    df_by_category = df_product_stats.groupBy("product_category_name") \
        .agg(
            count("order_id").alias("total_sales"),
            sum("price").alias("total_revenue"),
            avg("price").alias("avg_price"),
            avg("review_score").alias("avg_rating")
        ) \
        .filter(col("product_category_name").isNotNull()) \
        .orderBy(desc("total_revenue"))
    
    print("\n🏆 Top 10 Categories by Revenue:")
    df_by_category.show(10)
    
    return df_by_category

def compute_customer_segmentation(spark, df_orders, df_order_items, df_customers):
    """Segmenter les clients par comportement d'achat"""
    print("\n👥 Computing customer segmentation...")
    
    # RFM Analysis (Recency, Frequency, Monetary)
    df_rfm = df_orders \
        .join(df_order_items, "order_id") \
        .join(df_customers, "customer_id") \
        .groupBy("customer_id", "customer_state") \
        .agg(
            max("order_purchase_timestamp").alias("last_purchase_date"),
            count("order_id").alias("frequency"),
            sum(col("price") + col("freight_value")).alias("monetary_value")
        ) \
        .withColumn(
            "recency_days",
            datediff(current_date(), col("last_purchase_date"))
        )
    
    # Segmentation basique
    df_segments = df_rfm \
        .withColumn(
            "customer_segment",
            when((col("frequency") >= 3) & (col("monetary_value") >= 300), "VIP")
            .when((col("frequency") >= 2) & (col("monetary_value") >= 150), "Regular")
            .when(col("frequency") == 1, "One-time")
            .otherwise("Low-value")
        )
    
    # Distribution des segments
    print("\n📊 Customer Segments Distribution:")
    df_segments.groupBy("customer_segment") \
        .agg(
            count("customer_id").alias("count"),
            avg("monetary_value").alias("avg_spend")
        ) \
        .orderBy(desc("count")) \
        .show()
    
    return df_segments

def save_to_clickhouse(df, table_name, clickhouse_url="jdbc:clickhouse://clickhouse:8123/ecommerce"):
    """Sauvegarder DataFrame dans ClickHouse"""
    print(f"\n💾 Saving to ClickHouse: {table_name}")
    
    # Note: Nécessite le driver JDBC ClickHouse dans le classpath Spark
    df.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "password123") \
        .mode("overwrite") \
        .save()
    
    print(f"✅ Saved {df.count()} rows to {table_name}")

def save_to_parquet(df, path, partition_by=None):
    """Sauvegarder DataFrame en format Parquet"""
    print(f"\n💾 Saving to Parquet: {path}")
    
    # Ajouter le préfixe /opt/spark-apps/ si ce n'est pas déjà fait
    if not path.startswith('/opt/spark-apps/'):
        path = f"/opt/spark-apps/{path}"
    
    writer = df.write.mode("overwrite")
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.parquet(path)
    print(f"✅ Saved to {path}")

def main():
    """Point d'entrée principal"""
    print("🚀 Starting Spark Batch Processing...\n")
    
    # Créer session Spark
    spark = create_spark_session()
    
    try:
        # Charger les données
        df_orders, df_customers, df_products, df_order_items, df_reviews = load_csv_data(spark)
        
        # Agrégations de ventes
        df_by_state, df_by_month = compute_sales_aggregations(
            spark, df_orders, df_order_items, df_customers
        )
        
        # Métriques produits
        df_by_category = compute_product_metrics(
            spark, df_products, df_order_items, df_reviews
        )
        
        # Segmentation clients
        df_segments = compute_customer_segmentation(
            spark, df_orders, df_order_items, df_customers
        )
        
        # Sauvegarder les résultats en Parquet
        save_to_parquet(df_by_state, "data/parquet/sales_by_state")
        save_to_parquet(df_by_month, "data/parquet/sales_by_month", partition_by="order_month")
        save_to_parquet(df_by_category, "data/parquet/product_by_category")
        save_to_parquet(df_segments, "data/parquet/customer_segments")
        
        print("\n✅ Spark batch processing completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during Spark processing: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
