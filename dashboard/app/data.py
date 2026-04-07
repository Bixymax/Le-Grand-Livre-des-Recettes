"""
Connexion DuckDB et statistiques globales sans données mock.
"""

import os
import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "recipes_catalog.duckdb")

con = duckdb.connect(DB_PATH, read_only=True)
print("✅ DuckDB connecté à la base persistante")

# ---------------------------------------------------------------------------
# Initialisation des vues DuckDB
# ---------------------------------------------------------------------------

_recipes_main_path = os.path.join(DB_PATH, "recipes_main")
_nutrition_detail_path = os.path.join(DB_PATH, "recipes_nutrition_detail")

con.execute(f"""
    CREATE TABLE recipes_main AS
    SELECT * FROM read_parquet('{_recipes_main_path}/**/*.parquet', hive_partitioning=true)
""")
con.execute(f"""
    CREATE VIEW recipes_nutrition AS
    SELECT * FROM read_parquet('{_nutrition_detail_path}/**/*.parquet', hive_partitioning=true)
""")
print("✅ DuckDB connecté aux Parquets")

# Détecte si la colonne image_urls (tableau) existe dans recipes_main
_main_cols = {row[0] for row in con.execute("DESCRIBE recipes_main").fetchall()}
_image_urls_select = (
    "m.image_urls" if "image_urls" in _main_cols else "NULL::VARCHAR[] AS image_urls"
)

con.execute(f"""
    CREATE VIEW recipes AS
    SELECT m.*, {_image_urls_select},
           n.fat_g, n.protein_g, n.salt_g, n.sugars_g, n.saturates_g
    FROM recipes_main m
    LEFT JOIN recipes_nutrition n ON m.recipe_id = n.recipe_id
""")

# ---------------------------------------------------------------------------
# Statistiques globales
# ---------------------------------------------------------------------------

_stats = con.execute("""
    SELECT
        COUNT(*)                                                             AS total,
        COUNT(*) FILTER (WHERE has_image = true)                             AS with_image,
        COUNT(*) FILTER (WHERE energy_kcal IS NOT NULL)                      AS with_nutrition,
        ROUND(AVG(energy_kcal) FILTER (WHERE energy_kcal IS NOT NULL))       AS avg_kcal,
        ROUND(AVG(cook_minutes) FILTER (
            WHERE cook_minutes BETWEEN 1 AND 600))                           AS avg_cook,
        ROUND(100.0 * COUNT(*) FILTER (WHERE cook_time_category = 'rapide')
              / NULLIF(COUNT(*), 0))                                         AS pct_quick
    FROM recipes_main
""").fetchone()

TOTAL_RECIPES, TOTAL_WITH_IMAGE, TOTAL_WITH_NUTRITION, AVG_KCAL, AVG_COOK_MIN, PCT_QUICK = (
    int(_stats[0]), int(_stats[1]), int(_stats[2]),
    _stats[3] or 0, _stats[4] or 0, _stats[5] or 0,
)

_top_nutri = con.execute("""
    SELECT nutri_score FROM recipes_main WHERE nutri_score IS NOT NULL
    GROUP BY nutri_score ORDER BY COUNT(*) DESC LIMIT 1
""").fetchone()
TOP_NUTRI_SCORE = _top_nutri[0] if _top_nutri else "?"

PCT_A_B = con.execute("""
    SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE nutri_score IN ('A', 'B'))
                 / NULLIF(COUNT(*) FILTER (WHERE nutri_score IS NOT NULL), 0))
    FROM recipes_main
""").fetchone()[0] or 0

try:
    AVG_STEPS = (
        con.execute(
            "SELECT ROUND(AVG(n_steps)) FROM recipes_main WHERE n_steps IS NOT NULL"
        ).fetchone()[0] or 0
    )
except Exception:
    AVG_STEPS = 0

# ---------------------------------------------------------------------------
# Pré-calcul des IDs de recettes avec image
# ---------------------------------------------------------------------------

RECIPE_IDS_WITH_IMAGE = (
    con.execute("SELECT recipe_id FROM recipes_main WHERE has_image = true")
    .df()["recipe_id"]
    .tolist()
)

# ---------------------------------------------------------------------------
# Colonnes minimales pour l'affichage d'une recette
# ---------------------------------------------------------------------------

RECIPE_COLS = """
    m.recipe_id,
    m.title,
    m.instructions_text,
    m.ingredients_validated,
    m.cook_minutes,
    m.image_url,
    {image_urls_select},
    m.energy_kcal,
    m.nutri_score,
    n.fat_g,
    n.protein_g,
    n.sugars_g,
    n.salt_g,
    n.saturates_g
""".format(image_urls_select=_image_urls_select)