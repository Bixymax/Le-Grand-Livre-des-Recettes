"""
Phase 2b — Enrichissement et écriture des 3 tables finales Parquet.

Ce module prend le DataFrame assemblé (toutes colonnes brutes) et :
    1. Calcule les colonnes dérivées (``_enrich``).
    2. Construit les 3 tables finales via des fonctions dédiées.
    3. Les écrit en Parquet (batch) ou les prépare pour ``writeStream`` (streaming).

Tables produites :
    recipes_main              une ligne par recette
    ingredients_index         une ligne par (recette × ingrédient) — filtrage rapide
    recipes_nutrition_detail  détail kcal/100g par recette (source MIT uniquement)

Nutri-Score (calculé sur ``mit_energy_kcal``, kcal/100g, source MIT) :
    A < 80 | B < 160 | C < 270 | D < 400 | E ≥ 400
    Ne jamais utiliser ``kaggle_energy_kcal`` (kcal/portion) pour ce calcul.

Streaming :
    Passer ``streaming=True`` à ``write_final_tables`` pour basculer vers
    ``writeStream``. Requiert une SparkSession avec les extensions Delta Lake
    (voir ``spark_session.get_or_create_spark(streaming=True)``).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from le_grand_livre_des_recettes.pipeline import config as cfg


# ---------------------------------------------------------------------------
# Colonnes dérivées
# ---------------------------------------------------------------------------

def _nutri_score_col(kcal_col: str) -> F.Column:
    """
    Retourne une expression Spark calculant le Nutri-Score (A–E) depuis une
    colonne de kilocalories par 100g.

    Retourne ``null`` si la valeur source est nulle (recette sans données MIT).

    Parameters
    ----------
    kcal_col:
        Nom de la colonne kcal/100g dans le DataFrame.
    """
    c = F.col(kcal_col)
    return (
        F.when(c.isNull(), F.lit(None))
         .when(c < 80,     F.lit("A"))
         .when(c < 160,    F.lit("B"))
         .when(c < 270,    F.lit("C"))
         .when(c < 400,    F.lit("D"))
         .otherwise(       F.lit("E"))
    )


def _enrich(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes calculées sur le DataFrame assemblé brut.

    Colonnes ajoutées :
        - ``n_ingredients_validated``  compte les ingrédients validés (0 si null)
        - ``cook_time_category``       catégorise ``cook_minutes`` (rapide/moyen/long/inconnu)
        - ``has_image``                coalesce de ``has_image`` à ``False`` si null

    Parameters
    ----------
    df:
        DataFrame issu de ``assemble()``.
    """
    return (
        df
        .withColumn(
            "n_ingredients_validated",
            F.when(F.col("ingredients_validated").isNotNull(), F.size("ingredients_validated"))
             .otherwise(0),
        )
        .withColumn(
            "cook_time_category",
            F.when(F.col("cook_minutes") <= 30,          F.lit("rapide"))
             .when(F.col("cook_minutes") <= 60,          F.lit("moyen"))
             .when(F.col("cook_minutes").isNotNull(),    F.lit("long"))
             .otherwise(                                 F.lit("inconnu")),
        )
        .withColumn("has_image", F.coalesce(F.col("has_image"), F.lit(False)))
    )


# ---------------------------------------------------------------------------
# Construction des tables finales
# ---------------------------------------------------------------------------

def build_recipes_main(df: DataFrame) -> DataFrame:
    """
    Construit ``recipes_main`` : une ligne par recette, toutes colonnes utiles.

    Le partitionnement physique par ``nutri_score`` (A/B/C/D/E/null) accélère
    les requêtes filtrées par score côté moteur de recherche.

    Parameters
    ----------
    df:
        DataFrame enrichi (sortie de ``_enrich``).
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
            "mit_energy_kcal",                                          # kcal/100g  → Nutri-Score
            "kaggle_energy_kcal",                                        # kcal/portion → affichage
            _nutri_score_col("mit_energy_kcal").alias("nutri_score"),
            F.coalesce(F.col("tags"), F.array()).alias("tags"),
        )
        .dropDuplicates(["recipe_id"])
    )


def build_ingredients_index(df_main: DataFrame) -> DataFrame:
    """
    Construit ``ingredients_index`` : une ligne par (recette × ingrédient).

    Le pivot depuis ``ingredients_validated`` (array) vers des lignes individuelles
    permet des requêtes de filtrage par ingrédient sans scanner les arrays.

    Exemple :
        recipe_id | title          | ingredient
        abc111    | chocolate cake | flour
        abc111    | chocolate cake | sugar

    Parameters
    ----------
    df_main:
        DataFrame ``recipes_main`` (sortie de ``build_recipes_main``).
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
        .filter(F.col("ingredient").isNotNull() & (F.col("ingredient") != ""))
    )


def build_nutrition_detail(df: DataFrame) -> DataFrame:
    """
    Construit ``recipes_nutrition_detail`` : macronutriments g/100g par recette.

    Seules les recettes avec au moins une valeur nutritionnelle renseignée
    sont conservées (source MIT uniquement — aucune donnée Kaggle ici).

    Parameters
    ----------
    df:
        DataFrame assemblé brut (sortie de ``assemble()``).
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
# Orchestration de l'écriture
# ---------------------------------------------------------------------------

def write_final_tables(df_assembled: DataFrame) -> None:
    """
    Enrichit le DataFrame assemblé et écrit les 3 tables finales en Parquet.

    Stratégie d'écriture :
        - ``recipes_main``       : partitionné par ``nutri_score``, trié à l'intérieur
        - ``ingredients_index``  : non partitionné (évite les milliers de petits fichiers)
        - ``nutrition_detail``   : non partitionné (petite table, jointure par id)

    Parameters
    ----------
    df_assembled:
        DataFrame brut issu de ``assemble()``.
    """
    df_enriched = _enrich(df_assembled)

    df_main = build_recipes_main(df_enriched)
    (
        df_main
        .repartition(cfg.N_PARTITIONS, "nutri_score")
        .sortWithinPartitions("nutri_score")
        .write
        .mode("overwrite")
        .partitionBy("nutri_score")
        .parquet(cfg.OUT_RECIPES_MAIN)
    )
    print("  ✅ recipes_main          → outputs/parquets/recipes_main")

    df_index = build_ingredients_index(df_main)
    (
        df_index
        .write
        .mode("overwrite")
        .parquet(cfg.OUT_INGREDIENTS_INDEX)
    )
    print("  ✅ ingredients_index     → outputs/parquets/ingredients_index")

    df_nutr_detail = build_nutrition_detail(df_enriched)
    (
        df_nutr_detail
        .write
        .mode("overwrite")
        .parquet(cfg.OUT_NUTRITION_DETAIL)
    )
    print("  ✅ nutrition_detail      → outputs/parquets/recipes_nutrition_detail")