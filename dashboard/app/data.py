"""
Connexion DuckDB et statistiques globales sans données mock.
"""

import os
import duckdb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "..", "data", "../data/recipes_catalog.duckdb")

con = duckdb.connect(DB_PATH, read_only=True)
print("✅ DuckDB connecté à la base persistante")

_main_cols = {row[0] for row in con.execute("DESCRIBE recipes_main").fetchall()}
_image_urls_select = (
    "m.image_urls" if "image_urls" in _main_cols else "NULL::VARCHAR[] AS image_urls"
)

# ---------------------------------------------------------------------------
# Statistiques globales
# ---------------------------------------------------------------------------

_stats = con.cursor().execute("""
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

_top_nutri = con.cursor().execute("""
    SELECT nutri_score FROM recipes_main WHERE nutri_score IS NOT NULL
    GROUP BY nutri_score ORDER BY COUNT(*) DESC LIMIT 1
""").fetchone()
TOP_NUTRI_SCORE = _top_nutri[0] if _top_nutri else "?"

PCT_A_B = con.cursor().execute("""
    SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE nutri_score IN ('A', 'B'))
                 / NULLIF(COUNT(*) FILTER (WHERE nutri_score IS NOT NULL), 0))
    FROM recipes_main
""").fetchone()[0] or 0

try:
    AVG_STEPS = (
        con.cursor().execute(
            "SELECT ROUND(AVG(n_steps)) FROM recipes_main WHERE n_steps IS NOT NULL"
        ).fetchone()[0] or 0
    )
except Exception:
    AVG_STEPS = 0

# ---------------------------------------------------------------------------
# Pré-calcul des IDs de recettes avec image
# ---------------------------------------------------------------------------

random_id_df = con.cursor().execute("""
    SELECT recipe_id 
    FROM recipes_main 
    WHERE has_image = true 
    USING SAMPLE 1
""").df()

rid = random_id_df.iloc[0]["recipe_id"] if not random_id_df.empty else None

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
    m.image_urls,
    m.energy_kcal,
    m.nutri_score,
    n.fat_g,
    n.protein_g,
    n.sugars_g,
    n.salt_g,
    n.saturates_g
"""