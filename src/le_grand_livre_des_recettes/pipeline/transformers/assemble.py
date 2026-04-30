"""
Phase 2a — Assemblage des DataFrames staging.

Reproduit les jointures de 01_assemble.sql avec l'API DataFrame Spark.

Architecture des jointures :
  layer1 (base)
    LEFT JOIN layer2      ON id         → images
    LEFT JOIN det_ingrs   ON id         → ingrédients validés
    LEFT JOIN nutrition   ON title_norm → macronutriments (kcal/100g)
    LEFT JOIN kaggle      ON title_norm → description, cook_minutes, tags

Pourquoi LEFT JOIN partout :
  La recette de base (layer1) est toujours conservée, même si elle n'a
  pas d'image, de données nutritionnelles ou de correspondance Kaggle.

Dédoublonnage final :
  La jointure sur title_norm peut créer des doublons si deux recettes
  ont le même titre normalisé. On dédoublonne sur recipe_id (= layer1.id).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.le_grand_livre_des_recettes.pipeline import config as cfg


def assemble(spark: SparkSession) -> DataFrame:
    """
    Charge les Parquets staging et les assemble en un seul DataFrame enrichi.

    Returns
    -------
    DataFrame
        Toutes les colonnes des 5 sources, dédoublonnées sur recipe_id.
    """
    # --- Chargement des staging Parquets ---
    df_layer1 = spark.read.parquet(cfg.STAGING_LAYER1)
    df_layer2 = spark.read.parquet(cfg.STAGING_LAYER2)
    df_det    = spark.read.parquet(cfg.STAGING_DET_INGRS)
    df_nutr   = spark.read.parquet(cfg.STAGING_NUTR)
    df_kaggle = spark.read.parquet(cfg.STAGING_KAGGLE)

    # --- Jointures successives ---
    # On renomme les colonnes ambiguës avant les jointures sur title_norm
    # pour éviter les conflits (les deux côtés ont title_norm après les reads)
    df_nutr_renamed   = df_nutr.withColumnRenamed("title_norm", "nutr_title_norm")
    df_kaggle_renamed = df_kaggle.withColumnRenamed("title_norm", "kaggle_title_norm")

    df_assembled = (
        df_layer1
        .join(df_layer2,         on="id",          how="left")
        .join(df_det,            on="id",          how="left")
        .join(
            df_nutr_renamed,
            on=F.col("title_norm") == F.col("nutr_title_norm"),
            how="left",
        )
        .drop("nutr_title_norm")
        .join(
            df_kaggle_renamed,
            on=F.col("title_norm") == F.col("kaggle_title_norm"),
            how="left",
        )
        .drop("kaggle_title_norm")
    )

    # --- Enrichissement post-jointure ---
    df_enriched = (
        df_assembled
        # Nombre d'ingrédients validés (0 si colonne nulle)
        .withColumn(
            "n_ingredients_validated",
            F.when(
                F.col("ingredients_validated").isNotNull(),
                F.size("ingredients_validated"),
            ).otherwise(0),
        )
        # Catégorie de temps de cuisson
        .withColumn(
            "cook_time_category",
            F.when(F.col("cook_minutes") <= 30, F.lit("rapide"))
             .when(F.col("cook_minutes") <= 60, F.lit("moyen"))
             .when(F.col("cook_minutes").isNotNull(), F.lit("long"))
             .otherwise(F.lit("inconnu")),
        )
        # Énergie : priorité MIT (kcal/100g), fallback Kaggle (kcal/portion)
        # ATTENTION : le nutri_score sera calculé uniquement sur mit_energy_kcal
        .withColumnRenamed("energy_kcal", "mit_energy_kcal")
        # has_image peut venir de layer2 ou être calculé ici si null
        .withColumn(
            "has_image",
            F.coalesce(F.col("has_image"), F.lit(False)),
        )
    )

    # --- Dédoublonnage sur recipe_id ---
    return df_enriched.dropDuplicates(["id"])
