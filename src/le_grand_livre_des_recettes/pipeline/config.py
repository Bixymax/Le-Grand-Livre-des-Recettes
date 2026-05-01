"""
Configuration centrale du pipeline.

Structure des données attendue :

    data/
    ├── raw/            → fichiers sources (non versionnés, voir data/raw/README.md)
    ├── staging/        → Parquets intermédiaires écrits par dlt       (Phase 1, auto-créé)
    │   ├── layer1/
    │   ├── layer2/
    │   ├── det_ingrs/
    │   ├── nutrition/
    │   └── kaggle/
    └── outputs/        → tables finales Parquet écrites par PySpark   (Phase 2, auto-créé)
        └── parquets/
            ├── recipes_main/
            ├── ingredients_index/
            └── recipes_nutrition_detail/

Note sur les chemins staging :
    dlt (destination filesystem, dataset_name="staging") écrit les fichiers dans :
        {bucket_url}/{dataset_name}/{table_name}/{file_id}.parquet
    Avec bucket_url="data" → data/staging/{table_name}/{file_id}.parquet.
    PySpark lit ces répertoires avec spark.read.parquet(chemin_du_répertoire).

Note sur DLT_WRITE_DISPOSITION :
    - "replace" (défaut) : recharge complète à chaque exécution.
    - "append"           : chargement incrémental — à activer pour le streaming.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Racine du projet
# config.py est à : src/le_grand_livre_des_recettes/pipeline/config.py
# soit 4 niveaux sous la racine
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR     = PROJECT_ROOT / "data"

# ---------------------------------------------------------------------------
# Données sources (raw)
# ---------------------------------------------------------------------------

RAW_DIR        = DATA_DIR / "raw"

LAYER1_PATH    = RAW_DIR / "layer1.json"
LAYER2_PATH    = RAW_DIR / "layer2+.json"
DET_INGRS_PATH = RAW_DIR / "det_ingrs.json"
NUTR_PATH      = RAW_DIR / "recipes_with_nutritional_info.json"
RAW_CSV_PATH   = RAW_DIR / "RAW_recipes.csv"

# ---------------------------------------------------------------------------
# Staging — sorties dlt (Phase 1)
# ---------------------------------------------------------------------------

STAGING_DIR       = DATA_DIR / "staging"

STAGING_LAYER1    = str(STAGING_DIR / "layer1")
STAGING_LAYER2    = str(STAGING_DIR / "layer2")
STAGING_DET_INGRS = str(STAGING_DIR / "det_ingrs")
STAGING_NUTR      = str(STAGING_DIR / "nutrition")
STAGING_KAGGLE    = str(STAGING_DIR / "kaggle")

# ---------------------------------------------------------------------------
# Outputs finaux — sorties PySpark (Phase 2)
# ---------------------------------------------------------------------------

OUTPUT_DIR            = DATA_DIR / "outputs" / "parquets"

OUT_RECIPES_MAIN      = str(OUTPUT_DIR / "recipes_main")
OUT_INGREDIENTS_INDEX = str(OUTPUT_DIR / "ingredients_index")
OUT_NUTRITION_DETAIL  = str(OUTPUT_DIR / "recipes_nutrition_detail")

# ---------------------------------------------------------------------------
# Paramètres Spark
# ---------------------------------------------------------------------------

N_PARTITIONS = 8

# ---------------------------------------------------------------------------
# Paramètres dlt
# ---------------------------------------------------------------------------

# "replace" : recharge complète (défaut).
# "append"  : chargement incrémental — changer ici pour activer le streaming.
DLT_WRITE_DISPOSITION: str = "replace"
