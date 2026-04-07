import os
import duckdb
import time

# --- Configuration des chemins ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "..", "data", "recipes_parquets")
DB_PATH = os.path.join(BASE_DIR, "..", "data", "../data/recipes_catalog.duckdb")


def run_ingestion():
    start_time = time.time()
    print("🚀 Démarrage de l'ingestion...")

    if os.path.exists(DB_PATH):
        print(f"🗑️  Suppression de l'ancienne base : {DB_PATH}")
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)

    _recipes_main_path = os.path.join(DATA_PATH, "recipes_main")
    _nutrition_detail_path = os.path.join(DATA_PATH, "recipes_nutrition_detail")

    con.execute("INSTALL delta; LOAD delta;")

    print("📦 Importation de recipes_main...")
    con.execute(f"""
            CREATE TABLE recipes_main AS
            SELECT * FROM delta_scan('{_recipes_main_path}')
        """)

    print("📦 Importation de recipes_nutrition...")
    con.execute(f"""
            CREATE TABLE recipes_nutrition AS
            SELECT * FROM delta_scan('{_nutrition_detail_path}')
        """)

    # Index sur recipe_id pour des lookups par ID en O(log n) au lieu de O(n)
    print("🗂️  Création des index sur recipe_id...")
    con.execute("CREATE UNIQUE INDEX idx_main_recipe_id ON recipes_main(recipe_id);")
    con.execute("CREATE INDEX idx_nutrition_recipe_id ON recipes_nutrition(recipe_id);")

    print("🔗 Création de la vue 'recipes'...")
    con.execute("""
                CREATE VIEW recipes AS
                SELECT m.*,
                       n.fat_g,
                       n.protein_g,
                       n.salt_g,
                       n.sugars_g,
                       n.saturates_g
                FROM recipes_main m
                         LEFT JOIN recipes_nutrition n ON m.recipe_id = n.recipe_id
                """)

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