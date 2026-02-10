from sqlalchemy import text
from db_config_connection import engine  # on réutilise l'engine existant

def init_db():
    print("🛠 Création du schéma de base de données...")

    
    drop_tables = [
        "order_reviews",
        "order_payments",
        "order_items",
        "orders",
        "customers",
        "products",
        "sellers",
        "geolocation"
    ]

    # Tables à CREATE
    create_table_sqls = [
        ("geolocation", """CREATE TABLE geolocation (
            zip_code_prefix VARCHAR(10) PRIMARY KEY,
            geolocation_lat FLOAT,
            geolocation_lng FLOAT,
            geolocation_city VARCHAR(100),
            geolocation_state VARCHAR(5)
        );"""),
        ("sellers", """CREATE TABLE sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix VARCHAR(10),
            seller_city VARCHAR(100),
            seller_state VARCHAR(5)
        );"""),
        ("products", """CREATE TABLE products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_name_lenght FLOAT,
            product_description_lenght FLOAT,
            product_photos_qty FLOAT,
            product_weight_g FLOAT,
            product_length_cm FLOAT,
            product_height_cm FLOAT,
            product_width_cm FLOAT
        );"""),
        ("customers", """CREATE TABLE customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_zip_code_prefix VARCHAR(10),
            customer_city VARCHAR(100),
            customer_state VARCHAR(5)
        );"""),
        ("orders", """CREATE TABLE orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            order_status VARCHAR(50),
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );"""),
        ("order_items", """CREATE TABLE order_items (
            order_id VARCHAR(50),
            order_item_id INT,
            product_id VARCHAR(50),
            seller_id VARCHAR(50),
            shipping_limit_date TIMESTAMP,
            price FLOAT,
            freight_value FLOAT,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
        );"""),
        ("order_payments", """CREATE TABLE order_payments (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(50),
            payment_installments INT,
            payment_value FLOAT,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );"""),
        ("order_reviews", """CREATE TABLE order_reviews (
            review_id VARCHAR(50),
            order_id VARCHAR(50),
            review_score INT,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );""")
    ]

    with engine.begin() as conn:  
        # DROP tables
        for table in drop_tables:
            print(f"Suppression de {table} si elle existe...")
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))

        # CREATE tables
        for table_name, sql in create_table_sqls:
            print(f"Création de {table_name}...")
            conn.execute(text(sql))

    print("✅ Tables créées avec succès.")


# Exécuter directement le script
if __name__ == "__main__":
    init_db()
