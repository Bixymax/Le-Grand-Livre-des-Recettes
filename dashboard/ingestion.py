import os
import duckdb
import time

# --- Configuration des chemins ---
# À adapter selon l'arborescence exacte de ton projet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "..", "..", "data", "recipes_parquets")
DB_PATH = os.path.join(BASE_DIR, "recipes_catalog.duckdb")


def run_ingestion():
    start_time = time.time()
    print("🚀 Démarrage de l'ingestion...")

    # Si la base existe déjà, on la supprime pour faire une mise à jour propre
    if os.path.exists(DB_PATH):
        print(f"🗑️  Suppression de l'ancienne base : {DB_PATH}")
        os.remove(DB_PATH)

    # 1. Création de la base persistante
    con = duckdb.connect(DB_PATH)

    _recipes_main_path = os.path.join(DATA_PATH, "recipes_main")
    _nutrition_detail_path = os.path.join(DATA_PATH, "recipes_nutrition_detail")

    # 2. Importation des données dans des TABLES physiques
    print("📦 Importation de recipes_main (cela peut prendre un moment)...")
    con.execute(f"""
        CREATE TABLE recipes_main AS
        SELECT * FROM read_parquet('{_recipes_main_path}/**/*.parquet', hive_partitioning=true)
    """)

    print("📦 Importation de recipes_nutrition...")
    con.execute(f"""
        CREATE TABLE recipes_nutrition AS
        SELECT * FROM read_parquet('{_nutrition_detail_path}/**/*.parquet', hive_partitioning=true)
    """)

    # 3. Création de la vue combinée (pour simplifier tes requêtes Dash)
    print("🔗 Création de la vue 'recipes'...")
    _main_cols = {row[0] for row in con.execute("DESCRIBE recipes_main").fetchall()}
    _image_urls_select = "m.image_urls" if "image_urls" in _main_cols else "NULL::VARCHAR[] AS image_urls"

    con.execute(f"""
        CREATE VIEW recipes AS
        SELECT m.*, {_image_urls_select},
               n.fat_g, n.protein_g, n.salt_g, n.sugars_g, n.saturates_g
        FROM recipes_main m
        LEFT JOIN recipes_nutrition n ON m.recipe_id = n.recipe_id
    """)

    # 4. Création de l'index FTS (sur 'ingredients_validated' pour éviter l'erreur si 'raw' n'existe pas)
    print("🔍 Création de l'index de recherche FTS...")
    con.execute("INSTALL fts; LOAD fts;")
    con.execute("""
        PRAGMA create_fts_index(
            'recipes_main', 'recipe_id', 'title', 'ingredients_validated',
            stemmer  = 'french',
            stopwords= 'none',  
            lower    = 1,
            strip_accents = 1,
            overwrite= 1
        );
    """)

    con.close()
    elapsed = time.time() - start_time
    print(f"✅ Ingestion terminée en {elapsed:.1f} secondes ! Base prête : {DB_PATH}")


if __name__ == "__main__":
    run_ingestion()