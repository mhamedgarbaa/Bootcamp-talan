"""
Script Spark ML pour système de recommandation de produits
Utilise ALS (Alternating Least Squares) pour collaborative filtering
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import sys

def create_spark_session(app_name="Olist Recommendation Engine"):
    """Créer une session Spark avec MLlib"""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()

def load_and_prepare_data(spark, data_path="/opt/spark-apps/data/raw/"):
    """Charger et préparer les données pour le modèle ALS"""
    print("📥 Loading data for recommendation system...")
    
    # Charger orders + order_items + reviews
    df_orders = spark.read.csv(
        f"{data_path}olist_orders_dataset.csv",
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
    
    # Joindre: orders -> customer_id, order_items -> product_id, reviews -> score
    df_interactions = df_orders \
        .select("order_id", "customer_id") \
        .join(df_order_items.select("order_id", "product_id", "price"), "order_id") \
        .join(df_reviews.select("order_id", "review_score"), "order_id", "left")
    
    # Si pas de review_score, utiliser une valeur par défaut (3.0)
    df_interactions = df_interactions.fillna({"review_score": 3.0})
    
    # Créer un "rating" composite: review_score pondéré par le prix
    # Plus le prix est élevé, plus le poids est important
    df_interactions = df_interactions.withColumn(
        "rating",
        col("review_score") * (1 + log1p(col("price")) / 10)
    )
    
    print(f"✅ Loaded {df_interactions.count()} interactions")
    
    return df_interactions

def create_user_product_indices(df):
    """Créer des indices numériques pour customer_id et product_id"""
    print("\n🔢 Creating numerical indices...")
    
    # ALS nécessite des IDs numériques
    from pyspark.ml.feature import StringIndexer
    
    customer_indexer = StringIndexer(inputCol="customer_id", outputCol="customer_idx")
    product_indexer = StringIndexer(inputCol="product_id", outputCol="product_idx")
    
    df_indexed = customer_indexer.fit(df).transform(df)
    df_indexed = product_indexer.fit(df_indexed).transform(df_indexed)
    
    # Sélectionner les colonnes nécessaires
    df_indexed = df_indexed.select(
        "customer_id",
        "product_id",
        col("customer_idx").cast("int"),
        col("product_idx").cast("int"),
        "rating"
    )
    
    print(f"✅ Indexed {df_indexed.select('customer_idx').distinct().count()} unique customers")
    print(f"✅ Indexed {df_indexed.select('product_idx').distinct().count()} unique products")
    
    return df_indexed

def train_als_model(df_train):
    """Entraîner le modèle ALS"""
    print("\n🤖 Training ALS recommendation model...")
    
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="customer_idx",
        itemCol="product_idx",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True
    )
    
    model = als.fit(df_train)
    
    print("✅ Model trained successfully!")
    
    return model

def evaluate_model(model, df_test):
    """Évaluer la performance du modèle"""
    print("\n📊 Evaluating model...")
    
    predictions = model.transform(df_test)
    
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"✅ Root-Mean-Square Error (RMSE): {rmse:.3f}")
    
    return rmse

def generate_recommendations(model, df_indexed, top_n=10):
    """Générer des recommandations pour tous les clients"""
    print(f"\n🎯 Generating top {top_n} recommendations for each customer...")
    
    # Recommandations pour tous les utilisateurs
    user_recs = model.recommendForAllUsers(top_n)
    
    # Exploser les recommandations
    df_recs = user_recs.select(
        "customer_idx",
        explode("recommendations").alias("rec")
    ).select(
        "customer_idx",
        col("rec.product_idx").alias("product_idx"),
        col("rec.rating").alias("predicted_rating")
    )
    
    # Joindre avec les IDs originaux
    df_customer_map = df_indexed.select("customer_idx", "customer_id").distinct()
    df_product_map = df_indexed.select("product_idx", "product_id").distinct()
    
    df_recs = df_recs \
        .join(df_customer_map, "customer_idx") \
        .join(df_product_map, "product_idx") \
        .select("customer_id", "product_id", "predicted_rating")
    
    print(f"✅ Generated {df_recs.count()} recommendations")
    
    # Afficher quelques exemples
    print("\n📋 Sample recommendations:")
    df_recs.show(20, truncate=False)
    
    return df_recs

def save_recommendations(df_recs, output_path="/opt/spark-apps/data/parquet/recommendations"):
    """Sauvegarder les recommandations"""
    print(f"\n💾 Saving recommendations to {output_path}...")
    
    df_recs.coalesce(4) \
        .write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("✅ Recommendations saved successfully!")

def main():
    """Point d'entrée principal"""
    print("🚀 Starting Recommendation Engine with Spark ML...\n")
    
    # Créer session Spark
    spark = create_spark_session()
    
    try:
        # Charger et préparer les données
        df_interactions = load_and_prepare_data(spark)
        
        # Créer les indices numériques
        df_indexed = create_user_product_indices(df_interactions)
        
        # Split train/test (80/20)
        print("\n📊 Splitting data into train/test...")
        df_train, df_test = df_indexed.randomSplit([0.8, 0.2], seed=42)
        print(f"✅ Train: {df_train.count()} rows, Test: {df_test.count()} rows")
        
        # Entraîner le modèle
        model = train_als_model(df_train)
        
        # Évaluer le modèle
        rmse = evaluate_model(model, df_test)
        
        # Générer des recommandations
        df_recs = generate_recommendations(model, df_indexed, top_n=5)
        
        # Sauvegarder les recommandations
        save_recommendations(df_recs)
        
        # Statistiques finales
        print("\n📊 Recommendation Statistics:")
        df_recs.groupBy("customer_id") \
            .agg(
                count("product_id").alias("num_recommendations"),
                avg("predicted_rating").alias("avg_predicted_rating")
            ) \
            .show(10)
        
        print("\n✅ Recommendation system completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during recommendation generation: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
