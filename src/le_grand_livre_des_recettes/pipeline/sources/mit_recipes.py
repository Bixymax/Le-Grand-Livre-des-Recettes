"""
Source PySpark pour les fichiers MIT Recipe1M+.

Chaque fonction lit un fichier JSON avec un schéma Spark explicite
(équivalent aux StructType du notebook Databricks), applique les
transformations de normalisation, et écrit un fichier Parquet staging.

Transformations clés :
  - F.transform()  : extraction du texte des structures JSON imbriquées,
                     sans UDF → reste dans la JVM, plus performant.
  - arrays_zip()   : fusion des tableaux parallèles `ingredients` et `valid`
                     de det_ingrs.json pour filtrer les ingrédients validés.
  - title_norm     : clé de jointure normalisée (minuscules + sans ponctuation)
                     identique à celle du pipeline Databricks ET de la source
                     kaggle_recipes.py — indispensable pour la jointure croisée.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from src.le_grand_livre_des_recettes.pipeline import config as cfg

# ---------------------------------------------------------------------------
# Schémas Spark explicites
# Avantage vs inferSchema : pas de scan préalable du fichier, déterministe.
# ---------------------------------------------------------------------------

_LAYER1_SCHEMA = StructType([
    StructField("id",           StringType(), True),
    StructField("title",        StringType(), True),
    StructField("url",          StringType(), True),
    StructField("partition",    StringType(), True),
    StructField("instructions", ArrayType(StructType([
        StructField("text", StringType(), True),
    ])), True),
    StructField("ingredients",  ArrayType(StructType([
        StructField("text", StringType(), True),
    ])), True),
])

_LAYER2_SCHEMA = StructType([
    StructField("id",     StringType(), True),
    StructField("images", ArrayType(StructType([
        StructField("id",  StringType(), True),
        StructField("url", StringType(), True),
    ])), True),
])

_DET_INGRS_SCHEMA = StructType([
    StructField("id",          StringType(), True),
    StructField("ingredients", ArrayType(StructType([
        StructField("text", StringType(), True),
    ])), True),
    StructField("valid",       ArrayType(BooleanType()), True),
])

_NUTR_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("nutr_values_per100g", StructType([
        StructField("energy",    FloatType(), True),
        StructField("fat",       FloatType(), True),
        StructField("protein",   FloatType(), True),
        StructField("salt",      FloatType(), True),
        StructField("saturates", FloatType(), True),
        StructField("sugars",    FloatType(), True),
    ]), True),
])

# ---------------------------------------------------------------------------
# Macro de normalisation du titre (clé de jointure)
# Reproduit exactement : F.lower(F.trim(F.regexp_replace(col, r"[^a-zA-Z0-9\s]", "")))
# ---------------------------------------------------------------------------

def _normalize_title(col_name: str) -> "Column":  # noqa: F821
    return F.lower(F.trim(F.regexp_replace(col_name, r"[^a-zA-Z0-9\s]", "")))


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_layer1(spark: SparkSession) -> "DataFrame":  # noqa: F821
    """
    Lit layer1.json et retourne un DataFrame staging avec :
      - instructions_text : étapes concaténées avec " | "
      - ingredients_raw   : liste brute des textes d'ingrédients
      - title_norm        : clé de jointure normalisée
    """
    return (
        spark.read
        .option("multiLine", True)
        .schema(_LAYER1_SCHEMA)
        .json(str(cfg.LAYER1_PATH))
        # Concatène les étapes en une seule chaîne séparée par " | "
        .withColumn(
            "instructions_text",
            F.concat_ws(" | ", F.transform("instructions", lambda x: x["text"])),
        )
        # Extrait les textes d'ingrédients dans un Array[String] simple
        .withColumn(
            "ingredients_raw",
            F.transform("ingredients", lambda x: x["text"]),
        )
        .withColumn("n_steps", F.size("instructions"))
        .withColumn("title_norm", _normalize_title("title"))
        .drop("instructions", "ingredients")
    )


def read_layer2(spark: SparkSession) -> "DataFrame":  # noqa: F821
    """
    Lit layer2+.json et retourne un DataFrame staging avec :
      - image_url  : URL de la première image (ou null)
      - image_urls : liste de toutes les URLs d'images
      - has_image  : booléen
    """
    return (
        spark.read
        .option("multiLine", True)
        .schema(_LAYER2_SCHEMA)
        .json(str(cfg.LAYER2_PATH))
        .withColumn(
            "image_url",
            F.when(F.size("images") > 0, F.col("images")[0]["url"]),
        )
        .withColumn(
            "image_urls",
            F.transform("images", lambda x: x["url"]),
        )
        .withColumn("has_image", F.size("images") > 0)
        .drop("images")
    )


def read_det_ingrs(spark: SparkSession) -> "DataFrame":  # noqa: F821
    """
    Lit det_ingrs.json et retourne un DataFrame staging avec :
      - ingredients_validated    : liste des ingrédients dont valid == True
      - n_ingredients_validated  : longueur de cette liste

    Logique de filtrage :
      1. arrays_zip fusionne les tableaux parallèles `ingredients` et `valid`
         → [{"ingr_texts": "flour", "valid": true}, ...]
      2. F.filter ne conserve que les éléments valides
      3. F.transform extrait uniquement le texte
    """
    return (
        spark.read
        .option("multiLine", True)
        .schema(_DET_INGRS_SCHEMA)
        .json(str(cfg.DET_INGRS_PATH))
        # Étape 1 : textes bruts extraits
        .withColumn("ingr_texts", F.transform("ingredients", lambda x: x["text"]))
        # Étape 2 : fusion des deux tableaux parallèles
        .withColumn("zipped", F.arrays_zip("ingr_texts", "valid"))
        # Étape 3 : filtrage + extraction du texte uniquement
        .withColumn(
            "ingredients_validated",
            F.transform(
                F.filter("zipped", lambda x: x["valid"] == True),  # noqa: E712
                lambda x: F.lower(F.trim(x["ingr_texts"])),
            ),
        )
        .withColumn("n_ingredients_validated", F.size("ingredients_validated"))
        .drop("ingredients", "valid", "ingr_texts", "zipped")
    )


def read_nutrition(spark: SparkSession) -> "DataFrame":  # noqa: F821
    """
    Lit recipes_with_nutritional_info.json et aplatit le struct
    nutr_values_per100g en colonnes individuelles.

    Unités : kcal/100g (standard Nutri-Score européen).
    → Cette source est la SEULE utilisée pour calculer le nutri_score.
    """
    return (
        spark.read
        .option("multiLine", True)
        .schema(_NUTR_SCHEMA)
        .json(str(cfg.NUTR_PATH))
        .withColumn("energy_kcal",   F.col("nutr_values_per100g.energy"))
        .withColumn("fat_g",         F.col("nutr_values_per100g.fat"))
        .withColumn("protein_g",     F.col("nutr_values_per100g.protein"))
        .withColumn("salt_g",        F.col("nutr_values_per100g.salt"))
        .withColumn("saturates_g",   F.col("nutr_values_per100g.saturates"))
        .withColumn("sugars_g",      F.col("nutr_values_per100g.sugars"))
        .withColumn("title_norm",    _normalize_title("title"))
        .drop("nutr_values_per100g", "title")
    )


# ---------------------------------------------------------------------------
# Phase 1 : Écriture staging
# ---------------------------------------------------------------------------

def write_mit_staging(spark: SparkSession) -> None:
    """
    Lit les 4 fichiers MIT et les écrit en Parquet dans STAGING_DIR.
    C'est la Phase 1 du pipeline : on matérialise sur disque pour éviter
    de relire les JSON à chaque jointure de la Phase 2.
    """
    read_layer1(spark).write.mode("overwrite").parquet(cfg.STAGING_LAYER1)
    print("  ✅ layer1    → staging/layer1")

    read_layer2(spark).write.mode("overwrite").parquet(cfg.STAGING_LAYER2)
    print("  ✅ layer2    → staging/layer2")

    read_det_ingrs(spark).write.mode("overwrite").parquet(cfg.STAGING_DET_INGRS)
    print("  ✅ det_ingrs → staging/det_ingrs")

    read_nutrition(spark).write.mode("overwrite").parquet(cfg.STAGING_NUTR)
    print("  ✅ nutrition → staging/nutrition")
