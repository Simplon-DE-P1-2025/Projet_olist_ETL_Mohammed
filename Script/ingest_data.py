import pandas as pd
import os
from db_config_connection import engine  # on réutilise l'engine déjà défini
from Clean import CLEAN_DIR

def import_data():
    print(f"🚀 Démarrage de l'ingestion depuis : {CLEAN_DIR}")

    # Mapping fichier CSV → table PostgreSQL
    files_to_tables = {
        'geolocation.csv': 'geolocation',
        'sellers.csv': 'sellers',
        'products.csv': 'products',
        'customers.csv': 'customers',
        'orders.csv': 'orders',
        'order_items.csv': 'order_items',
        'order_payments.csv': 'order_payments',
        'order_reviews.csv': 'order_reviews'
    }

    for filename, table_name in files_to_tables.items():
        file_path = os.path.join(CLEAN_DIR, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠️ Fichier introuvable : {filename} (Ignoré)")
            continue

        print(f"📥 Ingestion de {table_name} depuis {filename}...")

        # Gestion spécifique des types pour éviter les erreurs sur les zip codes
        dtype_dict = {}
        if any(x in filename for x in ['zip', 'customer', 'seller', 'geolocation']):
            temp_df = pd.read_csv(file_path, nrows=1)
            for col in temp_df.columns:
                if 'zip' in col or 'id' in col:  # forcer zip et id en string
                    dtype_dict[col] = str

        # Lecture complète du CSV
        df = pd.read_csv(file_path, dtype=dtype_dict)

        # Insertion dans PostgreSQL
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000)
            print(f"   -> ✅ {len(df)} lignes insérées dans {table_name}.")
        except Exception as e:
            print(f"   -> ❌ Erreur sur {table_name} : {e}")


# Exécuter directement le script
if __name__ == "__main__":
    import_data()
