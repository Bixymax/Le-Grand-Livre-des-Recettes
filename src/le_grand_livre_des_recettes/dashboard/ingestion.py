"""
Script d'ingestion DuckDB depuis des fichiers Parquet.
"""

import os
import time
import duckdb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "../../..", "data", "outputs", "parquets")
DB_PATH = os.path.join(BASE_DIR, "../../..", "data", "outputs", "recipes_catalog.duckdb")


def run_ingestion():
    start_time = time.perf_counter()
    print("Démarrage de l'ingestion...")

    if os.path.exists(DB_PATH):
        print(f"Suppression de l'ancienne base : {DB_PATH}")
        os.remove(DB_PATH)

    with duckdb.connect(DB_PATH) as con:
        con.execute("INSTALL delta; LOAD delta;")
        con.execute("INSTALL fts; LOAD fts;")

        # Importation
        print("📦 Importation des tables principales...")
        main_path = os.path.join(DATA_PATH, "recipes_main")
        nutri_path = os.path.join(DATA_PATH, "recipes_nutrition_detail")

        con.execute(f"CREATE TABLE recipes_main AS SELECT * FROM delta_scan('{main_path}')")
        con.execute(f"CREATE TABLE recipes_nutrition AS SELECT * FROM delta_scan('{nutri_path}')")

        # Indexing (crucial pour le million de lignes)
        print("🗂️  Création des index (O(log n))...")
        con.execute("CREATE UNIQUE INDEX idx_main_recipe_id ON recipes_main(recipe_id);")
        con.execute("CREATE INDEX idx_nutrition_recipe_id ON recipes_nutrition(recipe_id);")

        print("Création de la vue analytique 'recipes'...")
        con.execute("""
                    CREATE VIEW recipes AS
                    SELECT m.*, n.fat_g, n.protein_g, n.salt_g, n.sugars_g, n.saturates_g
                    FROM recipes_main m
                             LEFT JOIN recipes_nutrition n ON m.recipe_id = n.recipe_id
                    """)

        # Full Text Search
        print("Création de l'index de recherche plein texte (FTS)...")
        con.execute("""
            PRAGMA create_fts_index(
                'recipes_main', 'recipe_id', 'title', 'ingredients_validated',
                stemmer='english', stopwords='none', lower=1, strip_accents=1, overwrite=1
            );
        """)

    elapsed = time.perf_counter() - start_time
    print(f"Ingestion terminée en {elapsed:.1f} secondes ! Base prête : {DB_PATH}")


if __name__ == "__main__":
    run_ingestion()