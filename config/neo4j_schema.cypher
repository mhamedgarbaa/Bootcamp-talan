// ========================================
// NEO4J GRAPH SCHEMA
// ========================================

// Contraintes d'unicité
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;

CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product) REQUIRE p.product_id IS UNIQUE;

CREATE CONSTRAINT seller_id_unique IF NOT EXISTS
FOR (s:Seller) REQUIRE s.seller_id IS UNIQUE;

CREATE CONSTRAINT order_id_unique IF NOT EXISTS
FOR (o:Order) REQUIRE o.order_id IS UNIQUE;

// Index pour performance
CREATE INDEX customer_state IF NOT EXISTS
FOR (c:Customer) ON (c.state);

CREATE INDEX product_category IF NOT EXISTS
FOR (p:Product) ON (p.category);

CREATE INDEX order_date IF NOT EXISTS
FOR (o:Order) ON (o.purchase_date);

// ========================================
// SCHÉMA DES NŒUDS
// ========================================

// Nœud: Customer
// Properties: customer_id, unique_id, city, state, zip_code, 
//             segment, lifetime_value, total_orders

// Nœud: Product
// Properties: product_id, category, name_length, description_length,
//             weight_g, price_avg

// Nœud: Seller
// Properties: seller_id, city, state, rating_avg, total_sales

// Nœud: Order
// Properties: order_id, purchase_date, status, total_amount,
//             payment_type, delivery_date

// Nœud: Category
// Properties: category_name, product_count

// ========================================
// SCHÉMA DES RELATIONS
// ========================================

// Customer -[:PLACED]-> Order
// Properties: order_date, order_status

// Order -[:CONTAINS]-> Product
// Properties: quantity, price, freight_value

// Seller -[:SELLS]-> Product
// Properties: first_sale_date, total_quantity_sold

// Product -[:BELONGS_TO]-> Category

// Customer -[:REVIEWED]-> Product
// Properties: review_score, review_date, sentiment

// Customer -[:BOUGHT_TOGETHER]-> Product
// Properties: co_purchase_count, confidence

// Seller -[:LOCATED_IN]-> City

// Customer -[:LOCATED_IN]-> City

// ========================================
// EXEMPLE DE REQUÊTES
// ========================================

// 1. Clients qui ont acheté des produits similaires (Recommendation)
// MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)<-[:CONTAINS]-(o2:Order)<-[:PLACED]-(c2:Customer)
// WHERE c <> c2
// RETURN c.customer_id, c2.customer_id, COUNT(DISTINCT p) as common_products
// ORDER BY common_products DESC
// LIMIT 10;

// 2. Chemin le plus court entre un client et un produit
// MATCH path = shortestPath((c:Customer {customer_id: 'CUST123'})-[*]-(p:Product {product_id: 'PROD456'}))
// RETURN path;

// 3. Produits fréquemment achetés ensemble (Market Basket)
// MATCH (p1:Product)<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(p2:Product)
// WHERE p1 <> p2
// RETURN p1.product_id, p2.product_id, COUNT(*) as frequency
// ORDER BY frequency DESC
// LIMIT 20;

// 4. Influence des sellers (PageRank)
// CALL gds.pageRank.stream('seller-product-graph')
// YIELD nodeId, score
// RETURN gds.util.asNode(nodeId).seller_id AS seller, score
// ORDER BY score DESC
// LIMIT 10;

// 5. Détection de communautés de clients
// CALL gds.louvain.stream('customer-product-graph')
// YIELD nodeId, communityId
// RETURN gds.util.asNode(nodeId).customer_id AS customer, communityId
// ORDER BY communityId;
