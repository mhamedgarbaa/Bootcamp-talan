// ========================================
// MONGODB COLLECTIONS & SCHEMAS
// ========================================

// Connexion
use ecommerce;

// Collection: Products (Document Store)
db.createCollection("products", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["product_id", "product_category", "name_length", "description_length"],
      properties: {
        product_id: { bsonType: "string" },
        product_category: { bsonType: "string" },
        name_length: { bsonType: "int" },
        description_length: { bsonType: "int" },
        photos_qty: { bsonType: "int" },
        weight_g: { bsonType: "int" },
        length_cm: { bsonType: "int" },
        height_cm: { bsonType: "int" },
        width_cm: { bsonType: "int" },
        attributes: { bsonType: "object" },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  }
});

// Collection: Reviews (avec nested documents)
db.createCollection("reviews", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["review_id", "order_id", "review_score"],
      properties: {
        review_id: { bsonType: "string" },
        order_id: { bsonType: "string" },
        customer_id: { bsonType: "string" },
        review_score: { bsonType: "int", minimum: 1, maximum: 5 },
        review_comment_title: { bsonType: ["string", "null"] },
        review_comment_message: { bsonType: ["string", "null"] },
        review_creation_date: { bsonType: "date" },
        review_answer_timestamp: { bsonType: ["date", "null"] },
        sentiment: {
          bsonType: "object",
          properties: {
            polarity: { bsonType: "double" },
            subjectivity: { bsonType: "double" },
            classification: { enum: ["positive", "neutral", "negative"] }
          }
        },
        order_details: {
          bsonType: "object",
          properties: {
            product_id: { bsonType: "string" },
            product_category: { bsonType: "string" },
            price: { bsonType: "decimal" },
            delivery_status: { bsonType: "string" }
          }
        }
      }
    }
  }
});

// Collection: Customers (avec historique embedded)
db.createCollection("customers", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["customer_id", "customer_unique_id"],
      properties: {
        customer_id: { bsonType: "string" },
        customer_unique_id: { bsonType: "string" },
        customer_zip_code_prefix: { bsonType: "string" },
        customer_city: { bsonType: "string" },
        customer_state: { bsonType: "string" },
        location: {
          bsonType: "object",
          properties: {
            type: { enum: ["Point"] },
            coordinates: { bsonType: "array" }
          }
        },
        profile: {
          bsonType: "object",
          properties: {
            segment: { enum: ["Champions", "Loyal", "At Risk", "Lost", "New"] },
            lifetime_value: { bsonType: "decimal" },
            total_orders: { bsonType: "int" },
            avg_order_value: { bsonType: "decimal" },
            last_purchase_date: { bsonType: "date" },
            signup_date: { bsonType: "date" }
          }
        },
        order_history: {
          bsonType: "array",
          items: {
            bsonType: "object",
            properties: {
              order_id: { bsonType: "string" },
              purchase_date: { bsonType: "date" },
              total_amount: { bsonType: "decimal" },
              status: { bsonType: "string" }
            }
          }
        }
      }
    }
  }
});

// Index pour performance
db.products.createIndex({ "product_category": 1 });
db.products.createIndex({ "product_id": 1 }, { unique: true });

db.reviews.createIndex({ "order_id": 1 });
db.reviews.createIndex({ "review_score": 1 });
db.reviews.createIndex({ "review_creation_date": -1 });
db.reviews.createIndex({ "sentiment.classification": 1 });

db.customers.createIndex({ "customer_id": 1 }, { unique: true });
db.customers.createIndex({ "customer_unique_id": 1 });
db.customers.createIndex({ "location": "2dsphere" });
db.customers.createIndex({ "profile.segment": 1 });
db.customers.createIndex({ "profile.last_purchase_date": -1 });

// Time-Series Collection pour events tracking
db.createCollection("customer_events", {
  timeseries: {
    timeField: "timestamp",
    metaField: "customer_id",
    granularity: "minutes"
  }
});

db.customer_events.createIndex({ "customer_id": 1, "timestamp": -1 });
