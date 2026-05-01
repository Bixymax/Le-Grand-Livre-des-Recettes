"""Tests d'intégration pour le pipeline de traitement de recettes.

Ce module exécute le pipeline de bout en bout sur des données de test
(3 recettes MIT, 1 Kaggle) et valide :
- Le calcul du Nutri-Score (basé uniquement sur l'énergie MIT en kcal/100g).
- L'indépendance des métriques Kaggle (kcal/portion) vis-à-vis du Nutri-Score.
- La gestion des recettes sans données MIT (Nutri-Score nul).
- L'exclusion des ingrédients non valides de l'index des ingrédients.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from le_grand_livre_des_recettes.pipeline.transformers.assemble import assemble
from le_grand_livre_des_recettes.pipeline.transformers.enrich import write_final_tables

_CFG = "le_grand_livre_des_recettes.pipeline.config"


@pytest.fixture(scope="session")
def output_dir(
    spark: SparkSession,
    staging_dir: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Exécute le pipeline complet et stocke les résultats dans un dossier temporaire.

    Cette fixture s'exécute une seule fois par session de test. Les chemins
    de configuration sont patchés pour utiliser le répertoire de staging
    et écrire dans le dossier temporaire.
    """
    out = tmp_path_factory.mktemp("integration_out")
    with (
        patch(f"{_CFG}.STAGING_LAYER1", str(staging_dir / "layer1")),
        patch(f"{_CFG}.STAGING_LAYER2", str(staging_dir / "layer2")),
        patch(f"{_CFG}.STAGING_DET_INGRS", str(staging_dir / "det_ingrs")),
        patch(f"{_CFG}.STAGING_NUTR", str(staging_dir / "nutrition")),
        patch(f"{_CFG}.STAGING_KAGGLE", str(staging_dir / "kaggle")),
        patch(f"{_CFG}.OUT_RECIPES_MAIN", str(out / "recipes_main")),
        patch(f"{_CFG}.OUT_INGREDIENTS_INDEX", str(out / "ingredients_index")),
        patch(f"{_CFG}.OUT_NUTRITION_DETAIL", str(out / "recipes_nutrition_detail")),
    ):
        write_final_tables(assemble(spark))
    return out


class TestRecipesMain:
    """Tests pour la table principale des recettes (recipes_main)."""

    def test_one_row_per_recipe(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie l'unicité des recettes dans la table principale."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        assert df.count() == 3
        assert df.count() == df.select("recipe_id").distinct().count()

    def test_has_image_flags(self, spark: SparkSession, output_dir: Path) -> None:
        """Valide l'indicateur de présence d'image pour chaque recette."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        rows = {r["recipe_id"]: r["has_image"] for r in df.collect()}
        assert rows["abc111"] is True
        assert rows["bcd222"] is True
        assert rows["cde333"] is False

    def test_cook_time_categories(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie l'attribution correcte de la catégorie de temps de cuisson."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        rows = {r["recipe_id"]: r["cook_time_category"] for r in df.collect()}
        assert rows["abc111"] == "moyen"
        assert rows["bcd222"] == "inconnu"
        assert rows["cde333"] == "inconnu"

    def test_valid_cook_time_values(self, spark: SparkSession, output_dir: Path) -> None:
        """S'assure que la catégorie de temps de cuisson utilise les valeurs attendues."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        cats = {r["cook_time_category"] for r in df.collect()}
        assert cats.issubset({"rapide", "moyen", "long", "inconnu"})


class TestEnergySemantics:
    """Tests pour la logique métier liée aux données énergétiques et au Nutri-Score."""

    def test_both_energy_columns_exist(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que les colonnes d'énergie MIT et Kaggle coexistent sans fusion."""
        cols = set(spark.read.parquet(str(output_dir / "recipes_main")).columns)
        assert "mit_energy_kcal" in cols
        assert "kaggle_energy_kcal" in cols
        assert "energy_kcal" not in cols

    def test_nutri_score_null_without_mit_data(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que le Nutri-Score est nul en l'absence de données MIT."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        row = df.filter(df.recipe_id == "bcd222").collect()[0]
        assert row["mit_energy_kcal"] is None
        assert row["nutri_score"] is None

    def test_nutri_score_uses_mit_kcal_per_100g(self, spark: SparkSession, output_dir: Path) -> None:
        """Valide que le Nutri-Score est calculé avec les données MIT (kcal/100g)."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        row = df.filter(df.recipe_id == "abc111").collect()[0]
        assert row["mit_energy_kcal"] == pytest.approx(350.0)
        assert row["kaggle_energy_kcal"] == pytest.approx(380.0)
        assert row["nutri_score"] == "D"

    def test_apple_pie_nutri_score(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie le calcul correct du Nutri-Score pour une recette spécifique (apple pie)."""
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        row = df.filter(df.recipe_id == "cde333").collect()[0]
        assert row["nutri_score"] == "C"


class TestIngredientsIndex:
    """Tests pour l'indexation des ingrédients."""

    def test_invalid_ingredient_excluded(self, spark: SparkSession, output_dir: Path) -> None:
        """S'assure que les ingrédients marqués comme invalides sont exclus de l'index."""
        df = spark.read.parquet(str(output_dir / "ingredients_index"))
        assert df.filter(df.ingredient == "pie crust").count() == 0

    def test_valid_ingredients_indexed(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que tous les ingrédients valides d'une recette sont indexés."""
        df = spark.read.parquet(str(output_dir / "ingredients_index"))
        ingrs = {r["ingredient"] for r in df.filter(df.recipe_id == "abc111").collect()}
        assert {"flour", "sugar", "eggs", "cocoa powder"}.issubset(ingrs)

    def test_filter_and(self, spark: SparkSession, output_dir: Path) -> None:
        """Valide la recherche de recettes contenant plusieurs ingrédients spécifiques."""
        df = spark.read.parquet(str(output_dir / "ingredients_index"))
        result = (
            df.filter(df.ingredient.isin("flour", "sugar"))
            .groupBy("recipe_id")
            .agg({"ingredient": "count"})
            .filter("`count(ingredient)` = 2")
            .collect()
        )
        assert {r["recipe_id"] for r in result} == {"abc111"}


class TestNutritionDetail:
    """Tests pour la table des détails nutritionnels."""

    def test_only_recipes_with_mit_data(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que seules les recettes avec des données MIT sont présentes."""
        df = spark.read.parquet(str(output_dir / "recipes_nutrition_detail"))
        ids = {r["recipe_id"] for r in df.collect()}
        assert ids == {"abc111", "cde333"}

    def test_pasta_absent(self, spark: SparkSession, output_dir: Path) -> None:
        """S'assure qu'une recette sans données nutritionnelles MIT est absente."""
        df = spark.read.parquet(str(output_dir / "recipes_nutrition_detail"))
        assert df.filter(df.recipe_id == "bcd222").count() == 0