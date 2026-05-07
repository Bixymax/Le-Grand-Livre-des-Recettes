"""
Configuration centrale du pipeline.

Définit les chemins des répertoires pour les données brutes (raw),
les données intermédiaires (staging) et les résultats finaux (outputs).
"""

from __future__ import annotations

from pathlib import Path

# Racine du projet
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
DATA_DIR: Path = PROJECT_ROOT / "data"

# Données sources (raw)
RAW_DIR: Path = DATA_DIR / "raw"

LAYER1_PATH: Path = RAW_DIR / "layer1.json"
LAYER2_PATH: Path = RAW_DIR / "layer2+.json"
DET_INGRS_PATH: Path = RAW_DIR / "det_ingrs.json"
NUTR_PATH: Path = RAW_DIR / "recipes_with_nutritional_info.json"
RAW_CSV_PATH: Path = RAW_DIR / "RAW_recipes.csv"

# Staging - sorties dlt en Delta Lake (Phase 1)
STAGING_DIR: Path = DATA_DIR / "staging"

STAGING_LAYER1: str = str(STAGING_DIR / "layer1")
STAGING_LAYER2: str = str(STAGING_DIR / "layer2")
STAGING_DET_INGRS: str = str(STAGING_DIR / "det_ingrs")
STAGING_NUTR: str = str(STAGING_DIR / "nutrition")
STAGING_KAGGLE: str = str(STAGING_DIR / "kaggle")

# Outputs finaux - sorties PySpark en Delta Lake (Phase 2)
OUTPUT_DIR: Path = DATA_DIR / "outputs" / "delta"

OUT_RECIPES_MAIN: str = str(OUTPUT_DIR / "recipes_main")
OUT_INGREDIENTS_INDEX: str = str(OUTPUT_DIR / "ingredients_index")
OUT_NUTRITION_DETAIL: str = str(OUTPUT_DIR / "recipes_nutrition_detail")

# Paramètres Spark
N_PARTITIONS: int = 8

# Paramètres dlt
DLT_WRITE_DISPOSITION: str = "replace"
