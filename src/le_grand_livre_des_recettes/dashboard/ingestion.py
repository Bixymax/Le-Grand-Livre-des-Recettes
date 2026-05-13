"""
Script d'ingestion DuckDB depuis des tables Delta Lake.
"""

import os
import time
import duckdb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "../../..", "data", "outputs", "delta")
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


def incremental_update_from_stream(con: duckdb.DuckDBPyConnection) -> int:
    """
    Insère dans recipes_main les recettes du stream absentes du catalogue.

    Utilise la connexion existante du dashboard pour éviter tout conflit
    de verrou DuckDB. Retourne le nombre de lignes insérées.
    """
    stream_path = os.path.join(DATA_PATH, "recipes_stream")
    if not os.path.isdir(os.path.join(stream_path, "_delta_log")):
        return 0

    try:
        con.execute("LOAD delta;")
    except Exception:
        con.execute("INSTALL delta; LOAD delta;")

    # Schéma cible
    describe = con.execute("DESCRIBE recipes_main").fetchall()
    main_cols = [row[0] for row in describe]
    main_types = {row[0]: row[1] for row in describe}

    # Colonnes disponibles dans la table stream
    stream_col_set = {
        row[0]
        for row in con.execute(f"DESCRIBE delta_scan('{stream_path}')").fetchall()
    }

    # Sélection : colonne commune → s.col, absente → NULL casté au bon type
    select_parts = [
        f"s.{col}" if col in stream_col_set else f"NULL::{main_types[col]} AS {col}"
        for col in main_cols
    ]

    before = con.execute("SELECT COUNT(*) FROM recipes_main").fetchone()[0]

    con.execute(f"""
        INSERT INTO recipes_main ({", ".join(main_cols)})
        SELECT {", ".join(select_parts)}
        FROM delta_scan('{stream_path}') s
        WHERE s.recipe_id NOT IN (SELECT recipe_id FROM recipes_main)
    """)

    inserted = con.execute("SELECT COUNT(*) FROM recipes_main").fetchone()[0] - before

    if inserted > 0:
        con.execute("""
            PRAGMA create_fts_index(
                'recipes_main', 'recipe_id', 'title', 'ingredients_validated',
                stemmer='english', stopwords='none', lower=1, strip_accents=1, overwrite=1
            );
        """)

    return inserted


if __name__ == "__main__":
    run_ingestion()