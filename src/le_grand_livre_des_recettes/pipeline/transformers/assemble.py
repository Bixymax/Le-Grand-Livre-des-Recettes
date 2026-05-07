"""
Phase 2a — Chargement des tables Delta staging et assemblage par jointures.

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


def _read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Charge une table Delta depuis le staging."""
    return spark.read.format("delta").load(path)


def _ensure_array(df: DataFrame, col: str) -> DataFrame:
    """
    Convertit une colonne en ArrayType[String] si elle est stockée en JSON string,
    et la laisse telle quelle si elle est déjà un array natif.

    dlt peut écrire les types `json` soit en chaînes JSON, soit en arrays natifs
    selon la version et le backend (deltalake/pyarrow), d'où ce garde-fou.
    """
    field = next(f for f in df.schema.fields if f.name == col)
    if isinstance(field.dataType, ArrayType):
        return df
    return df.withColumn(col, F.from_json(col, _ARRAY_OF_STRINGS))


def assemble(spark: SparkSession) -> DataFrame:
    """
    Charge les tables Delta staging, convertit les structures JSON en arrays typés,
    résout les conflits de colonnes (suppression de `n_steps` de Kaggle) et
    assemble le tout via des LEFT JOIN sur le DataFrame principal (`layer1`).
    """
    df_layer1 = _ensure_array(_read_delta(spark, cfg.STAGING_LAYER1), "ingredients_raw")
    df_layer2 = _ensure_array(_read_delta(spark, cfg.STAGING_LAYER2), "image_urls")
    df_det = _ensure_array(_read_delta(spark, cfg.STAGING_DET_INGRS), "ingredients_validated")

    # Préparation de la table nutritionnelle (MIT)
    df_nutr = (
        _read_delta(spark, cfg.STAGING_NUTR)
        .withColumnRenamed("title_norm", "nutr_title_norm")
        .withColumnRenamed("energy_kcal", "mit_energy_kcal")
        .drop("title")
        .dropDuplicates(["nutr_title_norm"])
    )

    # Préparation de la table Kaggle
    df_kaggle = (
        _ensure_array(_read_delta(spark, cfg.STAGING_KAGGLE), "tags")
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