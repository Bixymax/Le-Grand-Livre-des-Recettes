"""
Source PySpark pour le dataset Food.com / Kaggle (RAW_recipes.csv).

Les colonnes `tags` et `nutrition` sont stockées comme des chaînes de
caractères Python (ex: "['tag1', 'tag2']"). On les normalise ici avec
des regexp_replace + split identiques au notebook Databricks original.

ATTENTION — unité énergie :
  `kaggle_energy_kcal` est en kcal/PORTION (premier élément du champ
  `nutrition` de Food.com), pas en kcal/100g.
  Elle NE DOIT PAS être utilisée pour calculer le Nutri-Score.
  Utiliser `energy_kcal` (source MIT, kcal/100g) à la place.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType

from src.le_grand_livre_des_recettes.pipeline import config as cfg


def read_kaggle(spark: SparkSession) -> "DataFrame":  # noqa: F821
    """
    Lit RAW_recipes.csv et normalise les colonnes complexes.

    Transformations :
      - tags_raw   : chaîne "['tag1', 'tag2']" → Array[String] Spark
      - title_norm : clé de jointure identique à celle de mit_recipes.py
      - kaggle_energy_kcal : premier élément de la colonne `nutrition`
        (kcal/portion, ≠ kcal/100g)
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(cfg.RAW_CSV_PATH))
        .select(
            F.col("id").cast(StringType()).alias("kaggle_id"),
            F.col("minutes").cast(IntegerType()).alias("cook_minutes"),
            F.col("tags").alias("tags_raw"),
            F.col("nutrition").alias("nutrition_raw"),
            F.col("n_steps").cast(IntegerType()),
            F.col("description"),
            F.col("name").alias("name_kaggle"),
        )
        # Nettoyage "['tag1', 'tag2']" → ["tag1", "tag2"]
        .withColumn(
            "tags",
            F.split(
                F.regexp_replace(
                    F.regexp_replace("tags_raw", r"[\[\]']", ""),
                    r",\s+",
                    ",",
                ),
                ",",
            ),
        )
        # Clé de jointure normalisée — reproduit exactement la logique MIT
        .withColumn(
            "title_norm",
            F.lower(F.trim(F.regexp_replace("name_kaggle", r"[^a-zA-Z0-9\s]", ""))),
        )
        # Premier élément de "[kcal, fat, sugar, ...]" → kaggle_energy_kcal (kcal/portion)
        .withColumn(
            "nutrition_array",
            F.split(F.regexp_replace("nutrition_raw", r"[\[\]]", ""), ","),
        )
        .withColumn(
            "kaggle_energy_kcal",
            F.col("nutrition_array")[0].cast(FloatType()),
        )
        .drop("tags_raw", "name_kaggle", "nutrition_array", "nutrition_raw")
        # Dédoublonnage par title_norm (même logique que le notebook Databricks)
        .dropDuplicates(["title_norm"])
    )


def write_kaggle_staging(spark: SparkSession) -> None:
    """Lit RAW_recipes.csv et écrit le Parquet staging."""
    read_kaggle(spark).write.mode("overwrite").parquet(cfg.STAGING_KAGGLE)
    print("  ✅ kaggle    → staging/kaggle")
