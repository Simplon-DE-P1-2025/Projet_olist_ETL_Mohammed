import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "Script"))

import Script.Clean as Clean
from Script.create_tables import init_db
from Script.ingest_data import import_data
def main():
    print("=== 🚀 Pipeline de nettoyage et ingestion démarré ===\n")

    # Nettoyage des CSV
    print("--- Étape 1 : Nettoyage des fichiers CSV ---")
    Clean.clean_customers()
    Clean.clean_geolocation()
    Clean.clean_orders()
    Clean.clean_order_items()
    Clean.clean_payments()
    Clean.clean_reviews()
    Clean.clean_products()
    Clean.clean_sellers()
    print("--- Nettoyage terminé ! ---\n")

    # Création des tables
    print("--- Étape 2 : Création des tables PostgreSQL ---")
    init_db()
    print("--- Création des tables terminée ! ---\n")

    # Insertion des données
    print("--- Étape 3 : Insertion des données dans PostgreSQL ---")
    import_data()
    print("--- Ingestion terminée ! ---\n")

    print("=== ✅ Pipeline complet terminé avec succès ===")

if __name__ == "__main__":
    main()
