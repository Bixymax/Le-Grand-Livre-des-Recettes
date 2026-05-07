"""
Phase 2a — Chargement des Parquets staging et assemblage par jointures.

Assure le chargement des données staging, la conversion des colonnes JSON en arrays,
la résolution des conflits de nommage (ex: `energy_kcal` renommé en `mit_energy_kcal`),
et l'assemblage complet via des jointures LEFT sur `layer1`. Une déduplication
finale sur `id` garantit l'unicité des recettes malgré les jointures floues sur `title_norm`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from le_grand_livre_des_recettes.pipeline import config as cfg

_ARRAY_OF_STRINGS = ArrayType(StringType())


def assemble(spark: SparkSession) -> DataFrame:
    """
    Charge les Parquets staging, convertit les structures JSON en arrays typés,
    résout les conflits de colonnes (suppression de `n_steps` de Kaggle) et
    assemble le tout via des LEFT JOIN sur le DataFrame principal (`layer1`).
    """
    df_layer1 = spark.read.parquet(cfg.STAGING_LAYER1).withColumn(
        "ingredients_raw", F.from_json("ingredients_raw", _ARRAY_OF_STRINGS)
    )

    df_layer2 = spark.read.parquet(cfg.STAGING_LAYER2).withColumn(
        "image_urls", F.from_json("image_urls", _ARRAY_OF_STRINGS)
    )

    df_det = spark.read.parquet(cfg.STAGING_DET_INGRS).withColumn(
        "ingredients_validated",
        F.from_json("ingredients_validated", _ARRAY_OF_STRINGS),
    )

    # Préparation de la table nutritionnelle (MIT)
    df_nutr = (
        spark.read.parquet(cfg.STAGING_NUTR)
        .withColumnRenamed("title_norm", "nutr_title_norm")
        .withColumnRenamed("energy_kcal", "mit_energy_kcal")
        .drop("title")
        .dropDuplicates(["nutr_title_norm"])
    )

    # Préparation de la table Kaggle
    df_kaggle = (
        spark.read.parquet(cfg.STAGING_KAGGLE)
        .withColumn("tags", F.from_json("tags", _ARRAY_OF_STRINGS))
        .withColumnRenamed("title_norm", "kaggle_title_norm")
        .drop("n_steps")  # Conservé depuis layer1
        .dropDuplicates(["kaggle_title_norm"])
    )

    # Assemblage final
    return (
        df_layer1.join(df_layer2, on="id", how="left")
        .join(df_det, on="id", how="left")
        .join(
            df_nutr,
            on=F.col("title_norm") == F.col("nutr_title_norm"),
            how="left",
        )
        .drop("nutr_title_norm")
        .join(
            df_kaggle,
            on=F.col("title_norm") == F.col("kaggle_title_norm"),
            how="left",
        )
        .drop("kaggle_title_norm")
        .dropDuplicates(["id"])
    )