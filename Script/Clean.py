import pandas as pd
import os

# Configuration des chemins
RAW_DIR = './Data/'  # Dossier où sont vos CSV bruts
CLEAN_DIR = './cleaned_data/' # Dossier de sortie

if not os.path.exists(CLEAN_DIR):
    os.makedirs(CLEAN_DIR)

def clean_customers():
    print("Nettoyage: Customers...")
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_customers_dataset.csv'), dtype={'customer_zip_code_prefix': str})
    # Pas de nettoyage majeur nécessaire, juste s'assurer que le zip est un string
    df.to_csv(os.path.join(CLEAN_DIR, 'customers.csv'), index=False)

def clean_geolocation():
    print("Nettoyage: Geolocation (Agrégation)...")
    # Ce fichier contient beaucoup de doublons pour le même code postal.
    # On fait une moyenne des lat/lng pour avoir un point unique par code postal.
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_geolocation_dataset.csv'), dtype={'geolocation_zip_code_prefix': str})
    
    df_agg = df.groupby('geolocation_zip_code_prefix').agg({
        'geolocation_lat': 'mean',
        'geolocation_lng': 'mean',
        'geolocation_city': 'first', # On prend la première occurrence
        'geolocation_state': 'first'
    }).reset_index()
    # Filtrer les coordonnées aberrantes (Le Brésil est entre Lat -35/5 et Long -75/-34)
    df_agg = df_agg[df_agg['geolocation_lat'].between(-35, 5)]
    df_agg = df_agg[df_agg['geolocation_lng'].between(-75, -34)]
    # Renommer pour la cohérence
    df_agg.rename(columns={'geolocation_zip_code_prefix': 'zip_code_prefix'}, inplace=True)
    df_agg.to_csv(os.path.join(CLEAN_DIR, 'geolocation.csv'), index=False)

def clean_orders():
    print("Nettoyage: Orders...")
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_orders_dataset.csv'))
    
    # Conversion des colonnes de dates
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    # 2. Supprimer les commandes où la livraison est antérieure à l'achat (incohérence)
    mask_incoherent = df['order_delivered_customer_date'] < df['order_purchase_timestamp']
    df = df[~mask_incoherent]
        
    df.to_csv(os.path.join(CLEAN_DIR, 'orders.csv'), index=False)

def clean_order_items():
    print("Nettoyage: Order Items...")
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_order_items_dataset.csv'))
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'])
    df.to_csv(os.path.join(CLEAN_DIR, 'order_items.csv'), index=False)

def clean_payments():
    print("Nettoyage: Payments...")
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_order_payments_dataset.csv'))
    # Supprimer les prix négatifs ou nuls (impossible)
    df = df[df['payment_value'] > 0]
    df.to_csv(os.path.join(CLEAN_DIR, 'order_payments.csv'), index=False)

def clean_reviews():
    print("Nettoyage: Reviews...")
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_order_reviews_dataset.csv'))
    
    # Nettoyage du texte 
    df['review_comment_title'] = df['review_comment_title'].fillna('no comment')
    df['review_comment_message'] = df['review_comment_message'].fillna('no comment')
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'])
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'])
    
    df.to_csv(os.path.join(CLEAN_DIR, 'order_reviews.csv'), index=False)

def clean_products():
    print("Nettoyage: Products (avec traduction)...")
    df_prod = pd.read_csv(os.path.join(RAW_DIR, 'olist_products_dataset.csv'))
    df_trans = pd.read_csv(os.path.join(RAW_DIR, 'product_category_name_translation.csv'))
    
    # Fusion pour avoir les noms en anglais
    df_merged = pd.merge(df_prod, df_trans, on='product_category_name', how='left')
    
    # Si pas de traduction, on garde l'original ou 'unknown'
    df_merged['product_category_name_english'] = df_merged['product_category_name_english'].fillna(df_merged['product_category_name'])
    
    # Suppression de la colonne portugaise (optionnel, mais plus propre pour la base)
    df_merged.drop(columns=['product_category_name'], inplace=True)
    df_merged.rename(columns={'product_category_name_english': 'product_category_name'}, inplace=True)

    # Remplir les catégories manquantes par 'others'
    df_merged['product_category_name'] = df_merged['product_category_name'].fillna('others')
    
    df_merged.to_csv(os.path.join(CLEAN_DIR, 'products.csv'), index=False)

def clean_sellers():
    print("Nettoyage: Sellers...")
    df = pd.read_csv(os.path.join(RAW_DIR, 'olist_sellers_dataset.csv'), dtype={'seller_zip_code_prefix': str})
    df.to_csv(os.path.join(CLEAN_DIR, 'sellers.csv'), index=False)

if __name__ == "__main__":
    clean_customers()
    clean_geolocation()
    clean_orders()
    clean_order_items()
    clean_payments()
    clean_reviews()
    clean_products()
    clean_sellers()
    print("--- Nettoyage terminé ! Fichiers disponibles dans /cleaned_data ---")