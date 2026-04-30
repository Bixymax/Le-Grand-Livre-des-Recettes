"""
Phase 2b — Enrichissement et création des 3 tables finales.

Reproduit la logique de 02_final_tables.sql avec l'API DataFrame Spark.

Tables produites :
  1. recipes_main           : une ligne par recette, toutes les colonnes
  2. ingredients_index      : une ligne par (recette, ingrédient) — pivot
                              pour le filtrage rapide par ingrédient
  3. recipes_nutrition_detail : détail nutritionnel par recette (kcal/100g)

Nutri-Score :
  Calculé uniquement sur mit_energy_kcal (kcal/100g, source MIT).
  Ne jamais utiliser kaggle_energy_kcal (kcal/portion) pour ce calcul.

Seuils Nutri-Score simplifiés (kcal/100g) :
  A < 80 | B < 160 | C < 270 | D < 400 | E ≥ 400
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.le_grand_livre_des_recettes.pipeline import config as cfg


# ---------------------------------------------------------------------------
# Macro Nutri-Score (identique à 02_final_tables.sql)
# ---------------------------------------------------------------------------

def _nutri_score_col(kcal_col: str) -> "Column":  # noqa: F821
    """Retourne une colonne Spark avec le Nutri-Score (A–E ou null)."""
    c = F.col(kcal_col)
    return (
        F.when(c.isNull(),   F.lit(None))
         .when(c < 80,       F.lit("A"))
         .when(c < 160,      F.lit("B"))
         .when(c < 270,      F.lit("C"))
         .when(c < 400,      F.lit("D"))
         .otherwise(         F.lit("E"))
    )


# ---------------------------------------------------------------------------
# Table 1 : recipes_main
# ---------------------------------------------------------------------------

def build_recipes_main(df: DataFrame) -> DataFrame:
    """
    Construit recipes_main : une ligne par recette avec toutes les colonnes
    utiles à l'affichage et au filtrage.

    Partitionnement physique sur nutri_score (A–E + null) :
      → Réduit les données lues lors d'un filtre par nutri_score.
      → Cohérent avec le partitionBy("nutri_score") du notebook Databricks.
    """
    return (
        df.select(
            F.col("id").alias("recipe_id"),
            "title",
            "description",
            "instructions_text",
            "ingredients_raw",
            "ingredients_validated",
            "n_ingredients_validated",
            "n_steps",
            "cook_minutes",
            "cook_time_category",
            "image_url",
            "image_urls",
            "has_image",
            F.col("url").alias("source_url"),
            # Énergie — unités séparées, ne pas mélanger
            "mit_energy_kcal",      # kcal/100g  → Nutri-Score
            "kaggle_energy_kcal",   # kcal/portion → affichage uniquement
            _nutri_score_col("mit_energy_kcal").alias("nutri_score"),
            F.coalesce(F.col("tags"), F.array()).alias("tags"),
        )
        .dropDuplicates(["recipe_id"])
    )


# ---------------------------------------------------------------------------
# Table 2 : ingredients_index
# ---------------------------------------------------------------------------

def build_ingredients_index(df_main: DataFrame) -> DataFrame:
    """
    Construit ingredients_index en "pivotant" la liste ingredients_validated :
      une ligne = un ingrédient × une recette.

    C'est la transformation explode_outer du notebook Databricks.
    Permet des requêtes de filtrage par ingrédient ultra-rapides sans
    scanner l'intérieur des arrays.

    Exemple :
      recipe_id | title         | ingredient
      abc111    | chocolate cake | flour
      abc111    | chocolate cake | sugar
      abc111    | chocolate cake | eggs
    """
    return (
        df_main
        .select(
            "recipe_id",
            "title",
            "nutri_score",
            "image_url",
            "cook_time_category",
            F.explode_outer("ingredients_validated").alias("ingredient"),
        )
        .withColumn("ingredient", F.lower(F.trim("ingredient")))
        .filter(
            F.col("ingredient").isNotNull() &
            (F.col("ingredient") != "")
        )
    )


# ---------------------------------------------------------------------------
# Table 3 : recipes_nutrition_detail
# ---------------------------------------------------------------------------

def build_nutrition_detail(df: DataFrame) -> DataFrame:
    """
    Construit recipes_nutrition_detail : détail nutritionnel par recette.
    Ne conserve que les recettes ayant au moins une valeur nutritionnelle.
    Toutes les unités sont en g/100g (source MIT).
    """
    return (
        df.select(
            F.col("id").alias("recipe_id"),
            "fat_g",
            "protein_g",
            "salt_g",
            "saturates_g",
            "sugars_g",
        )
        .filter(
            F.col("fat_g").isNotNull()       |
            F.col("protein_g").isNotNull()   |
            F.col("salt_g").isNotNull()      |
            F.col("saturates_g").isNotNull() |
            F.col("sugars_g").isNotNull()
        )
        .dropDuplicates(["recipe_id"])
    )


# ---------------------------------------------------------------------------
# Phase 2 : Écriture des tables finales
# ---------------------------------------------------------------------------

def write_final_tables(df_assembled: DataFrame) -> None:
    """
    Construit et écrit les 3 tables finales en Parquet.

    Stratégie d'écriture :
      - recipes_main       : partitionné par nutri_score pour accélérer
                             les filtres par score (A/B/C/D/E).
      - ingredients_index  : non partitionné (évite des milliers de petits
                             fichiers, un par ingrédient), identique au
                             commentaire du notebook Databricks original.
      - nutrition_detail   : non partitionné (petite table, jointure par id).
    """
    # --- Table 1 : recipes_main ---
    df_main = build_recipes_main(df_assembled)
    (
        df_main
        .repartition(cfg.N_PARTITIONS, "nutri_score")
        .write
        .mode("overwrite")
        .partitionBy("nutri_score")
        .parquet(cfg.OUT_RECIPES_MAIN)
    )
    print("  ✅ recipes_main          → outputs/parquets/recipes_main")

    # --- Table 2 : ingredients_index ---
    df_index = build_ingredients_index(df_main)
    (
        df_index
        .write
        .mode("overwrite")
        .parquet(cfg.OUT_INGREDIENTS_INDEX)
    )
    print("  ✅ ingredients_index     → outputs/parquets/ingredients_index")

    # --- Table 3 : nutrition_detail ---
    df_nutr_detail = build_nutrition_detail(df_assembled)
    (
        df_nutr_detail
        .write
        .mode("overwrite")
        .parquet(cfg.OUT_NUTRITION_DETAIL)
    )
    print("  ✅ nutrition_detail      → outputs/parquets/recipes_nutrition_detail")
