"""
Tests d'intégration — pipeline complet sur les fixtures (3 recettes MIT + 1 Kaggle).

Vérifie la sémantique de bout en bout :
  - Nutri-Score calculé sur mit_energy_kcal (kcal/100g) uniquement
  - kaggle_energy_kcal (kcal/portion) jamais utilisé pour le score
  - Recettes sans données MIT → nutri_score null
  - Ingrédients valid=False exclus de l'index
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
    """Lance le pipeline complet une seule fois pour toute la session."""
    out = tmp_path_factory.mktemp("integration_out")
    with (
        patch(f"{_CFG}.STAGING_LAYER1",          str(staging_dir / "layer1")),
        patch(f"{_CFG}.STAGING_LAYER2",          str(staging_dir / "layer2")),
        patch(f"{_CFG}.STAGING_DET_INGRS",       str(staging_dir / "det_ingrs")),
        patch(f"{_CFG}.STAGING_NUTR",            str(staging_dir / "nutrition")),
        patch(f"{_CFG}.STAGING_KAGGLE",          str(staging_dir / "kaggle")),
        patch(f"{_CFG}.OUT_RECIPES_MAIN",        str(out / "recipes_main")),
        patch(f"{_CFG}.OUT_INGREDIENTS_INDEX",   str(out / "ingredients_index")),
        patch(f"{_CFG}.OUT_NUTRITION_DETAIL",    str(out / "recipes_nutrition_detail")),
    ):
        write_final_tables(assemble(spark))
    return out


# ---------------------------------------------------------------------------
# recipes_main
# ---------------------------------------------------------------------------

class TestRecipesMain:
    def test_one_row_per_recipe(self, spark: SparkSession, output_dir: Path) -> None:
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        assert df.count() == 3
        assert df.count() == df.select("recipe_id").distinct().count()

    def test_has_image_flags(self, spark: SparkSession, output_dir: Path) -> None:
        """abc111 et bcd222 ont des images, cde333 non."""
        df   = spark.read.parquet(str(output_dir / "recipes_main"))
        rows = {r["recipe_id"]: r["has_image"] for r in df.collect()}
        assert rows["abc111"] is True
        assert rows["bcd222"] is True
        assert rows["cde333"] is False

    def test_cook_time_categories(self, spark: SparkSession, output_dir: Path) -> None:
        """abc111 a cook_minutes=45 (Kaggle) → moyen. Sans match Kaggle → inconnu."""
        df   = spark.read.parquet(str(output_dir / "recipes_main"))
        rows = {r["recipe_id"]: r["cook_time_category"] for r in df.collect()}
        assert rows["abc111"] == "moyen"
        assert rows["bcd222"] == "inconnu"
        assert rows["cde333"] == "inconnu"

    def test_valid_cook_time_values(self, spark: SparkSession, output_dir: Path) -> None:
        df   = spark.read.parquet(str(output_dir / "recipes_main"))
        cats = {r["cook_time_category"] for r in df.collect()}
        assert cats.issubset({"rapide", "moyen", "long", "inconnu"})


# ---------------------------------------------------------------------------
# Sémantique énergie — cœur de la logique métier
# ---------------------------------------------------------------------------

class TestEnergySemantics:
    def test_both_energy_columns_exist(self, spark: SparkSession, output_dir: Path) -> None:
        """Les deux colonnes doivent coexister sans COALESCE — les unités sont incompatibles."""
        cols = set(spark.read.parquet(str(output_dir / "recipes_main")).columns)
        assert "mit_energy_kcal"    in cols
        assert "kaggle_energy_kcal" in cols
        assert "energy_kcal"        not in cols

    def test_nutri_score_null_without_mit_data(self, spark: SparkSession, output_dir: Path) -> None:
        """
        bcd222 (pasta bolognese) : pas de données MIT.
        nutri_score doit être null même si kaggle_energy_kcal est présent.
        """
        df  = spark.read.parquet(str(output_dir / "recipes_main"))
        row = df.filter(df.recipe_id == "bcd222").collect()[0]
        assert row["mit_energy_kcal"] is None
        assert row["nutri_score"]     is None

    def test_nutri_score_uses_mit_kcal_per_100g(self, spark: SparkSession, output_dir: Path) -> None:
        """
        abc111 : mit_energy_kcal=350 kcal/100g → score D (270–399).
        kaggle_energy_kcal=380 kcal/portion ne doit PAS influer sur le score.
        """
        df  = spark.read.parquet(str(output_dir / "recipes_main"))
        row = df.filter(df.recipe_id == "abc111").collect()[0]
        assert row["mit_energy_kcal"]    == pytest.approx(350.0)
        assert row["kaggle_energy_kcal"] == pytest.approx(380.0)
        assert row["nutri_score"]        == "D"

    def test_apple_pie_nutri_score(self, spark: SparkSession, output_dir: Path) -> None:
        """cde333 : mit_energy_kcal=237 kcal/100g → score C (160–269)."""
        df  = spark.read.parquet(str(output_dir / "recipes_main"))
        row = df.filter(df.recipe_id == "cde333").collect()[0]
        assert row["nutri_score"] == "C"


# ---------------------------------------------------------------------------
# ingredients_index
# ---------------------------------------------------------------------------

class TestIngredientsIndex:
    def test_invalid_ingredient_excluded(self, spark: SparkSession, output_dir: Path) -> None:
        """pie crust a valid=False dans det_ingrs.json → absent de l'index."""
        df = spark.read.parquet(str(output_dir / "ingredients_index"))
        assert df.filter(df.ingredient == "pie crust").count() == 0

    def test_valid_ingredients_indexed(self, spark: SparkSession, output_dir: Path) -> None:
        df    = spark.read.parquet(str(output_dir / "ingredients_index"))
        ingrs = {r["ingredient"] for r in df.filter(df.recipe_id == "abc111").collect()}
        assert {"flour", "sugar", "eggs", "cocoa powder"}.issubset(ingrs)

    def test_filter_and(self, spark: SparkSession, output_dir: Path) -> None:
        """Recettes contenant 'flour' ET 'sugar' → uniquement abc111."""
        df     = spark.read.parquet(str(output_dir / "ingredients_index"))
        result = (
            df.filter(df.ingredient.isin("flour", "sugar"))
            .groupBy("recipe_id")
            .agg({"ingredient": "count"})
            .filter("`count(ingredient)` = 2")
            .collect()
        )
        assert {r["recipe_id"] for r in result} == {"abc111"}


# ---------------------------------------------------------------------------
# recipes_nutrition_detail
# ---------------------------------------------------------------------------

class TestNutritionDetail:
    def test_only_recipes_with_mit_data(self, spark: SparkSession, output_dir: Path) -> None:
        """Seules abc111 et cde333 ont des données MIT → 2 lignes."""
        df  = spark.read.parquet(str(output_dir / "recipes_nutrition_detail"))
        ids = {r["recipe_id"] for r in df.collect()}
        assert ids == {"abc111", "cde333"}

    def test_pasta_absent(self, spark: SparkSession, output_dir: Path) -> None:
        df = spark.read.parquet(str(output_dir / "recipes_nutrition_detail"))
        assert df.filter(df.recipe_id == "bcd222").count() == 0