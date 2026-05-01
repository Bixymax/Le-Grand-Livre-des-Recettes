"""
Phase 2a — Chargement des Parquets staging et assemblage par jointures.

Jointures :
    layer1  (base, clé : id)
    LEFT JOIN layer2      ON id         → images
    LEFT JOIN det_ingrs   ON id         → ingrédients validés
    LEFT JOIN nutrition   ON title_norm → macronutriments kcal/100g  (jointure floue sur titre)
    LEFT JOIN kaggle      ON title_norm → description, cook_minutes, tags

Toutes les jointures sont LEFT pour garantir que chaque recette de layer1
est conservée même sans image, données nutritionnelles ou correspondance Kaggle.

Gestion des conflits de colonnes :
    - ``energy_kcal`` (nutrition) est renommé en ``mit_energy_kcal`` pour
      distinguer les kcal/100g MIT des kcal/portion Kaggle.
    - ``n_steps`` (kaggle) est supprimé : la valeur de référence vient de layer1.
    - Les colonnes ``title_norm`` de nutrition et kaggle sont renommées avant
      la jointure pour éviter les ambiguïtés.

Déduplication finale sur ``id`` pour éliminer les doublons créés par les
jointures floues sur ``title_norm`` (plusieurs recettes peuvent partager
un titre normalisé identique).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from le_grand_livre_des_recettes.pipeline import config as cfg

_ARRAY_OF_STRINGS = ArrayType(StringType())


def assemble(spark: SparkSession) -> DataFrame:
    """
    Charge les Parquets staging et les assemble en un seul DataFrame.

    Les colonnes de type complexe (arrays) sont stockées par dlt comme des
    chaînes JSON ; ``from_json`` les convertit en vrais ``ArrayType(StringType())``.

    Parameters
    ----------
    spark:
        Session Spark active.

    Returns
    -------
    DataFrame
        Colonnes de toutes les sources assemblées, dédoublonnées sur ``id``.
        ``energy_kcal`` de nutrition est renommé ``mit_energy_kcal``.
    """
    df_layer1 = (
        spark.read.parquet(cfg.STAGING_LAYER1)
        .withColumn("ingredients_raw", F.from_json("ingredients_raw", _ARRAY_OF_STRINGS))
    )
    df_layer2 = (
        spark.read.parquet(cfg.STAGING_LAYER2)
        .withColumn("image_urls", F.from_json("image_urls", _ARRAY_OF_STRINGS))
    )
    df_det = (
        spark.read.parquet(cfg.STAGING_DET_INGRS)
        .withColumn("ingredients_validated", F.from_json("ingredients_validated", _ARRAY_OF_STRINGS))
    )
    df_nutr = (
        spark.read.parquet(cfg.STAGING_NUTR)
        .withColumnRenamed("title_norm", "nutr_title_norm")
        .withColumnRenamed("energy_kcal", "mit_energy_kcal")
        .drop("title")
        .dropDuplicates(["nutr_title_norm"])
    )
    df_kaggle = (
        spark.read.parquet(cfg.STAGING_KAGGLE)
        .withColumn("tags", F.from_json("tags", _ARRAY_OF_STRINGS))
        .withColumnRenamed("title_norm", "kaggle_title_norm")
        .drop("n_steps")  # n_steps de référence vient de layer1
        .dropDuplicates(["kaggle_title_norm"])
    )

    return (
        df_layer1
        .join(df_layer2, on="id", how="left")
        .join(df_det,    on="id", how="left")
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