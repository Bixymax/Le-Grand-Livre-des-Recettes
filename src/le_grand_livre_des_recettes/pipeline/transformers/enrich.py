"""
Phase 2b — Enrichissement et écriture des 3 tables finales Parquet.

Prend le DataFrame assemblé brut pour calculer les colonnes dérivées
et construire les tables `recipes_main`, `ingredients_index`, et
`recipes_nutrition_detail` avant l'écriture. Gère le partitionnement
sur le Nutri-Score pour l'optimisation des requêtes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from le_grand_livre_des_recettes.pipeline import config as cfg


def _nutri_score_col(kcal_col: str) -> F.Column:
    """
    Calcule le Nutri-Score (A–E) à partir des kilocalories pour 100g.
    Retourne `null` si la valeur source est manquante.
    """
    c = F.col(kcal_col)
    return (
        F.when(c.isNull(), F.lit(None))
        .when(c < 80, F.lit("A"))
        .when(c < 160, F.lit("B"))
        .when(c < 270, F.lit("C"))
        .when(c < 400, F.lit("D"))
        .otherwise(F.lit("E"))
    )


def _enrich(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes dérivées (comptage des ingrédients,
    catégorisation du temps de cuisson et flag image) au DataFrame.
    """
    return (
        df.withColumn(
            "n_ingredients_validated",
            F.when(
                F.col("ingredients_validated").isNotNull(),
                F.size("ingredients_validated")
            ).otherwise(0)
        )
        .withColumn(
            "cook_time_category",
            F.when(F.col("cook_minutes") <= 30, F.lit("rapide"))
            .when(F.col("cook_minutes") <= 60, F.lit("moyen"))
            .when(F.col("cook_minutes").isNotNull(), F.lit("long"))
            .otherwise(F.lit("inconnu"))
        )
        .withColumn("has_image", F.coalesce(F.col("has_image"), F.lit(False)))
    )


def build_recipes_main(df: DataFrame) -> DataFrame:
    """
    Construit la table principale `recipes_main` (une ligne par recette).
    Dédoublonne par `recipe_id` et intègre le calcul du Nutri-Score.
    """
    return df.select(
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
        "mit_energy_kcal",
        "kaggle_energy_kcal",
        _nutri_score_col("mit_energy_kcal").alias("nutri_score"),
        F.coalesce(F.col("tags"), F.array()).alias("tags"),
    ).dropDuplicates(["recipe_id"])


def build_ingredients_index(df_main: DataFrame) -> DataFrame:
    """
    Construit l'index `ingredients_index` par éclatement (explode)
    des ingrédients validés pour permettre un filtrage rapide.
    """
    return (
        df_main.select(
            "recipe_id",
            "title",
            "nutri_score",
            "image_url",
            "cook_time_category",
            F.explode_outer("ingredients_validated").alias("ingredient"),
        )
        .withColumn("ingredient", F.lower(F.trim("ingredient")))
        .filter(F.col("ingredient").isNotNull() & (F.col("ingredient") != ""))
    )


def build_nutrition_detail(df: DataFrame) -> DataFrame:
    """
    Construit la table `recipes_nutrition_detail` contenant le détail
    des macronutriments (g/100g, source MIT).
    Filtre les recettes ne possédant aucune donnée nutritionnelle.
    """
    return df.select(
        F.col("id").alias("recipe_id"),
        "fat_g",
        "protein_g",
        "salt_g",
        "saturates_g",
        "sugars_g",
    ).filter(
        F.col("fat_g").isNotNull()
        | F.col("protein_g").isNotNull()
        | F.col("salt_g").isNotNull()
        | F.col("saturates_g").isNotNull()
        | F.col("sugars_g").isNotNull()
    ).dropDuplicates(["recipe_id"])


def _zorder_optimize(spark: SparkSession, path: str, col: str) -> None:
    """Applique OPTIMIZE ZORDER BY sur une table Delta. Échoue silencieusement hors Databricks."""
    try:
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({col})")
        print(f"  [OK] Z-Order ({col})")
    except Exception as e:
        print(f"  [WARN] Z-Order ignoré ({col}) : {e}")


def write_final_tables(df_assembled: DataFrame) -> None:
    """
    Orchestre l'enrichissement et l'écriture en Delta des 3 tables finales.
    La table `recipes_main` est partitionnée physiquement par Nutri-Score.
    Chaque table reçoit ensuite un OPTIMIZE ZORDER BY pour accélérer les requêtes fréquentes.
    """
    spark = df_assembled.sparkSession
    df_enriched = _enrich(df_assembled)

    # 1. Construction et écriture de recipes_main
    df_main = build_recipes_main(df_enriched)
    (
        df_main.repartition(cfg.N_PARTITIONS, "nutri_score")
        .sortWithinPartitions("nutri_score")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("nutri_score")
        .save(cfg.OUT_RECIPES_MAIN)
    )
    _zorder_optimize(spark, cfg.OUT_RECIPES_MAIN, "title")
    print("  [OK] recipes_main          -> outputs/parquets/recipes_main")

    # 2. Construction et écriture de ingredients_index
    df_index = build_ingredients_index(df_main)
    df_index.write.format("delta").mode("overwrite").save(cfg.OUT_INGREDIENTS_INDEX)
    _zorder_optimize(spark, cfg.OUT_INGREDIENTS_INDEX, "ingredient")
    print("  [OK] ingredients_index     -> outputs/parquets/ingredients_index")

    # 3. Construction et écriture de nutrition_detail
    df_nutr_detail = build_nutrition_detail(df_enriched)
    df_nutr_detail.write.format("delta").mode("overwrite").save(cfg.OUT_NUTRITION_DETAIL)
    _zorder_optimize(spark, cfg.OUT_NUTRITION_DETAIL, "recipe_id")
    print("  [OK] nutrition_detail      -> outputs/parquets/recipes_nutrition_detail")