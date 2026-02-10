import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Variables Base de données
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST', 'localhost')  # localhost par défaut
DB_PORT = os.getenv('DB_PORT', '5432')       # 5432 par défaut
DB_NAME = os.getenv('DB_NAME')

# Vérifier que toutes les variables sont bien définies
if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
    raise ValueError("Il manque une variable d'environnement pour la DB. Vérifie ton fichier .env.")

# Construction de l'URL de connexion SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Création de l'objet engine SQLAlchemy
engine = create_engine(DATABASE_URL)

# Test de connexion
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()
        print(f"✅ Connecté à PostgreSQL : {version[0]}")
except Exception as e:
    print("❌ Impossible de se connecter à la base PostgreSQL :", e)
