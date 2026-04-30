"""
Configuration centrale du pipeline PySpark.

Tous les chemins sont relatifs à la racine du projet (DATA_DIR).
La structure reflète l'organisation des données dans le cluster Docker :
  data/
    raw/        → fichiers sources (JSON + CSV)
    staging/    → Parquet intermédiaires (Phase 1)
    outputs/    → tables finales Parquet (Phase 2)
      parquets/
        recipes_main/
        ingredients_index/
        recipes_nutrition_detail/
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Racine du projet
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[5]  # remonte jusqu'à recipes_pipeline/
DATA_DIR     = PROJECT_ROOT / "data"

# ---------------------------------------------------------------------------
# Données sources (raw)
# ---------------------------------------------------------------------------

RAW_DIR           = DATA_DIR / "raw"

LAYER1_PATH       = RAW_DIR / "layer1.json"
LAYER2_PATH       = RAW_DIR / "layer2+.json"
DET_INGRS_PATH    = RAW_DIR / "det_ingrs.json"
NUTR_PATH         = RAW_DIR / "recipes_with_nutritional_info.json"
RAW_CSV_PATH      = RAW_DIR / "RAW_recipes.csv"

# ---------------------------------------------------------------------------
# Staging (Parquet intermédiaires)
# ---------------------------------------------------------------------------

STAGING_DIR       = DATA_DIR / "staging"

STAGING_LAYER1    = str(STAGING_DIR / "layer1")
STAGING_LAYER2    = str(STAGING_DIR / "layer2")
STAGING_DET_INGRS = str(STAGING_DIR / "det_ingrs")
STAGING_NUTR      = str(STAGING_DIR / "nutrition")
STAGING_KAGGLE    = str(STAGING_DIR / "kaggle")

# ---------------------------------------------------------------------------
# Outputs finaux (Parquet)
# ---------------------------------------------------------------------------

OUTPUT_DIR              = DATA_DIR / "outputs" / "parquets"

OUT_RECIPES_MAIN        = str(OUTPUT_DIR / "recipes_main")
OUT_INGREDIENTS_INDEX   = str(OUTPUT_DIR / "ingredients_index")
OUT_NUTRITION_DETAIL    = str(OUTPUT_DIR / "recipes_nutrition_detail")

# ---------------------------------------------------------------------------
# Paramètres Spark
# ---------------------------------------------------------------------------

N_PARTITIONS = 8   # Identique au notebook Databricks original
