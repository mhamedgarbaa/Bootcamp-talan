"""
Dashboard Streamlit pour visualiser les données e-commerce
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import clickhouse_connect
from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.ERROR)

# Configuration de la page
st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Style CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Connexions aux bases de données
@st.cache_resource
def init_connections():
    """Initialise les connexions aux bases de données"""
    connections = {}
    
    try:
        mongo_client = MongoClient('mongodb://admin:password123@localhost:27017/')
        connections['mongodb'] = mongo_client['ecommerce']
        st.sidebar.success("✅ MongoDB connected")
    except Exception as e:
        st.sidebar.error(f"❌ MongoDB: {str(e)[:50]}")
        connections['mongodb'] = None
    
    try:
        clickhouse_client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='admin',
            password='password123',
            database='ecommerce'
        )
        connections['clickhouse'] = clickhouse_client
        st.sidebar.success("✅ ClickHouse connected")
    except Exception as e:
        st.sidebar.error(f"❌ ClickHouse: {str(e)[:50]}")
        connections['clickhouse'] = None
    
    try:
        neo4j_driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password123")
        )
        connections['neo4j'] = neo4j_driver
        st.sidebar.success("✅ Neo4j connected")
    except Exception as e:
        st.sidebar.error(f"❌ Neo4j: {str(e)[:50]}")
        connections['neo4j'] = None
    
    return connections

# Chargement des données
@st.cache_data(ttl=300)
def load_sales_data(_clickhouse_client):
    """Charge les données de ventes depuis ClickHouse"""
    if _clickhouse_client is None:
        return pd.DataFrame()
    
    query = """
        SELECT 
            toDate(order_purchase_timestamp) as order_date,
            COUNT(DISTINCT order_id) as orders,
            SUM(price + freight_value) as revenue,
            AVG(price + freight_value) as avg_order_value
        FROM ecommerce.sales_fact
        GROUP BY order_date
        ORDER BY order_date DESC
        LIMIT 10000
    """
    
    result = _clickhouse_client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    df['order_date'] = pd.to_datetime(df['order_date'])
    return df

@st.cache_data(ttl=300)
def load_reviews_data(_mongo_db):
    """Charge les reviews depuis MongoDB"""
    if _mongo_db is None:
        return pd.DataFrame()
    
    pipeline = [
        {
            "$group": {
                "_id": "$review_score",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    
    data = list(_mongo_db.reviews.aggregate(pipeline))
    df = pd.DataFrame(data)
    df.columns = ['review_score', 'count']
    return df

@st.cache_data(ttl=300)
def load_categories_data(_mongo_db):
    """Charge les catégories de produits depuis MongoDB"""
    if _mongo_db is None:
        return pd.DataFrame()
    
    pipeline = [
        {
            "$group": {
                "_id": "$product_category_name",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 15}
    ]
    
    data = list(_mongo_db.products.aggregate(pipeline))
    df = pd.DataFrame(data)
    df.columns = ['category', 'count']
    return df

# Interface principale
def main():
    # Header
    st.markdown('<h1 class="main-header">📊 E-Commerce Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialisation des connexions
    connections = init_connections()
    
    # Sidebar
    st.sidebar.title("🎛️ Options")
    page = st.sidebar.radio(
        "Navigation",
        ["📈 Overview", "💰 Sales Analytics", "⭐ Reviews Analysis", "🔗 Graph Analysis"]
    )
    
    # Pages
    if page == "📈 Overview":
        show_overview(connections)
    elif page == "💰 Sales Analytics":
        show_sales_analytics(connections)
    elif page == "⭐ Reviews Analysis":
        show_reviews_analysis(connections)
    elif page == "🔗 Graph Analysis":
        show_graph_analysis(connections)

def show_overview(connections):
    """Affiche la vue d'ensemble"""
    st.header("📊 Vue d'ensemble")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # KPIs depuis ClickHouse
    if connections['clickhouse']:
        try:
            result = connections['clickhouse'].query("""
                SELECT 
                    COUNT(DISTINCT order_id) as total_orders,
                    COUNT(DISTINCT customer_id) as total_customers,
                    COUNT(DISTINCT product_id) as total_products,
                    SUM(price + freight_value) as total_revenue
                FROM ecommerce.sales_fact
            """)
            row = result.first_row
            
            with col1:
                st.metric("📦 Total Orders", f"{row[0]:,}")
            with col2:
                st.metric("👥 Total Customers", f"{row[1]:,}")
            with col3:
                st.metric("🛍️ Total Products", f"{row[2]:,}")
            with col4:
                st.metric("💰 Total Revenue", f"${row[3]:,.2f}")
        except Exception as e:
            st.error(f"Error loading KPIs: {e}")
    
    # Graphiques
    st.subheader("📈 Ventes dans le temps")
    
    if connections['clickhouse']:
        df_sales = load_sales_data(connections['clickhouse'])
        
        if not df_sales.empty:
            # Agrégation par date
            df_daily = df_sales.groupby('order_date').agg({
                'orders': 'sum',
                'revenue': 'sum'
            }).reset_index()
            
            fig = px.line(
                df_daily,
                x='order_date',
                y='revenue',
                title='Revenue quotidien',
                labels={'order_date': 'Date', 'revenue': 'Revenue ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Graphique Revenue avec moyenne mobile
            st.subheader("📊 Analyse du Revenue")
            df_sales['revenue_ma7'] = df_sales['revenue'].rolling(window=7).mean()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_sales['order_date'],
                y=df_sales['revenue'],
                name='Revenue',
                mode='lines'
            ))
            fig.add_trace(go.Scatter(
                x=df_sales['order_date'],
                y=df_sales['revenue_ma7'],
                name='Moyenne Mobile (7j)',
                mode='lines',
                line=dict(dash='dash')
            ))
            fig.update_layout(
                title='Evolution du Revenue',
                xaxis_title='Date',
                yaxis_title='Revenue ($)'
            )
            st.plotly_chart(fig, use_container_width=True)

def show_sales_analytics(connections):
    """Affiche les analytics de ventes"""
    st.header("💰 Analyse des Ventes")
    
    if connections['clickhouse'] is None:
        st.warning("ClickHouse non connecté")
        return
    
    df_sales = load_sales_data(connections['clickhouse'])
    
    if df_sales.empty:
        st.warning("Aucune donnée disponible")
        return
    
    # Statistiques descriptives
    st.subheader("📊 Statistiques")
    df_filtered = df_sales.copy()
    
    # Métriques
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📦 Commandes", f"{df_filtered['orders'].sum():,}")
    with col2:
        st.metric("💰 Revenue", f"${df_filtered['revenue'].sum():,.2f}")
    with col3:
        st.metric("📊 Panier Moyen", f"${df_filtered['avg_order_value'].mean():,.2f}")
    
    # Graphiques
    df_daily = df_filtered.groupby('order_date')['revenue'].sum().reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_daily['order_date'],
        y=df_daily['revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#1f77b4', width=2)
    ))
    fig.update_layout(
        title='Evolution du Revenue',
        xaxis_title='Date',
        yaxis_title='Revenue ($)',
        hovermode='x'
    )
    st.plotly_chart(fig, use_container_width=True)

def show_reviews_analysis(connections):
    """Affiche l'analyse des reviews"""
    st.header("⭐ Analyse des Avis Clients")
    
    if connections['mongodb'] is None:
        st.warning("MongoDB non connecté")
        return
    
    df_reviews = load_reviews_data(connections['mongodb'])
    
    if df_reviews.empty:
        st.warning("Aucune donnée disponible")
        return
    
    # Métriques
    total_reviews = df_reviews['count'].sum()
    avg_score = (df_reviews['review_score'] * df_reviews['count']).sum() / total_reviews
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📝 Total Reviews", f"{total_reviews:,}")
    with col2:
        st.metric("⭐ Note Moyenne", f"{avg_score:.2f}/5")
    with col3:
        positive_pct = df_reviews[df_reviews['review_score'] >= 4]['count'].sum() / total_reviews * 100
        st.metric("✅ Avis Positifs", f"{positive_pct:.1f}%")
    
    # Distribution des notes
    fig = px.bar(
        df_reviews,
        x='review_score',
        y='count',
        title='Distribution des Notes',
        labels={'review_score': 'Note', 'count': 'Nombre d\'avis'},
        color='review_score',
        color_continuous_scale='RdYlGn'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Top/Bottom produits
    st.subheader("📊 Catégories de Produits")
    df_categories = load_categories_data(connections['mongodb'])
    
    if not df_categories.empty:
        fig = px.pie(
            df_categories,
            values='count',
            names='category',
            title='Répartition des Produits par Catégorie'
        )
        st.plotly_chart(fig, use_container_width=True)

def show_graph_analysis(connections):
    """Affiche l'analyse de graphe"""
    st.header("🔗 Analyse de Graphe")
    
    if connections['neo4j'] is None:
        st.warning("Neo4j non connecté")
        return
    
    try:
        with connections['neo4j'].session() as session:
            # Stats des nœuds
            st.subheader("📊 Statistiques du Graphe")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                result = session.run("MATCH (c:Customer) RETURN COUNT(c) as count")
                count = result.single()['count']
                st.metric("👥 Customers", f"{count:,}")
            
            with col2:
                result = session.run("MATCH (p:Product) RETURN COUNT(p) as count")
                count = result.single()['count']
                st.metric("📦 Products", f"{count:,}")
            
            with col3:
                result = session.run("MATCH ()-[r:PURCHASED]->() RETURN COUNT(r) as count")
                count = result.single()['count']
                st.metric("🔗 Purchases", f"{count:,}")
            
            # Top clients
            st.subheader("🏆 Top 10 Clients")
            result = session.run("""
                MATCH (c:Customer)-[r:PURCHASED]->()
                RETURN c.customer_id as customer, c.city as city, COUNT(r) as purchases
                ORDER BY purchases DESC
                LIMIT 10
            """)
            
            data = [dict(record) for record in result]
            df_top_customers = pd.DataFrame(data)
            
            if not df_top_customers.empty:
                fig = px.bar(
                    df_top_customers,
                    x='customer',
                    y='purchases',
                    title='Clients avec le plus d\'achats',
                    labels={'customer': 'Client', 'purchases': 'Nombre d\'achats'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Recommandations
            st.subheader("💡 Système de Recommandation")
            
            # Afficher des exemples de customer_ids
            st.info("Entrez un ID client pour obtenir des recommandations de produits basées sur les achats similaires")
            
            # Récupérer quelques customer IDs en exemple
            result_sample = session.run("""
                MATCH (c:Customer)-[r:PURCHASED]->()
                RETURN c.customer_id as id, COUNT(r) as purchases
                ORDER BY purchases DESC
                LIMIT 3
            """)
            sample_ids = [r['id'] for r in result_sample]
            if sample_ids:
                st.caption(f"💡 Exemples de customer IDs valides: {', '.join(sample_ids[:3])}")
            
            customer_id = st.text_input("Customer ID")
            if customer_id:
                # Vérifier d'abord si le customer existe
                check_result = session.run("""
                    MATCH (c:Customer {customer_id: $customer_id})
                    OPTIONAL MATCH (c)-[r:PURCHASED]->(p:Product)
                    RETURN c.customer_id as id, COUNT(r) as purchases
                """, customer_id=customer_id)
                
                customer_data = check_result.single()
                
                if not customer_data or customer_data['id'] is None:
                    st.error(f"❌ Customer '{customer_id}' n'existe pas dans la base de données.")
                    st.info("Utilisez un des exemples ci-dessus ou consultez le graphique 'Top 10 Clients' pour des IDs valides.")
                elif customer_data['purchases'] == 0:
                    st.warning(f"⚠️ Le customer '{customer_id}' n'a effectué aucun achat.")
                else:
                    st.info(f"✅ Customer trouvé avec {customer_data['purchases']} achat(s)")
                    
                    # Requête de recommandation
                    result = session.run("""
                        MATCH (c:Customer {customer_id: $customer_id})-[:PURCHASED]->(p:Product)
                        <-[:PURCHASED]-(c2:Customer)-[:PURCHASED]->(rec:Product)
                        WHERE NOT (c)-[:PURCHASED]->(rec)
                        RETURN rec.product_id as product, rec.category as category, COUNT(*) as score
                        ORDER BY score DESC
                        LIMIT 5
                    """, customer_id=customer_id)
                    
                    recommendations = [dict(record) for record in result]
                    if recommendations:
                        st.success(f"✅ {len(recommendations)} produits recommandés")
                        st.table(pd.DataFrame(recommendations))
                    else:
                        st.warning("Aucune recommandation trouvée pour ce client")
                        st.caption("Raisons possibles: aucun autre client n'a acheté les mêmes produits, ou tous les produits similaires ont déjà été achetés.")
    
    except Exception as e:
        st.error(f"Erreur: {e}")

if __name__ == "__main__":
    main()
