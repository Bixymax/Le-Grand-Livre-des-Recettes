"""Tests d'intégration Spark — assemble() + write_final_tables() via patch de config."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType

from le_grand_livre_des_recettes.pipeline.transformers.assemble import assemble
from le_grand_livre_des_recettes.pipeline.transformers.enrich import write_final_tables

_CFG = "le_grand_livre_des_recettes.pipeline.config"


def _patch_staging(staging_dir: Path):
    """Patch les 5 constantes de chemin staging vers le répertoire temporaire."""
    return (
        patch(f"{_CFG}.STAGING_LAYER1",    str(staging_dir / "layer1")),
        patch(f"{_CFG}.STAGING_LAYER2",    str(staging_dir / "layer2")),
        patch(f"{_CFG}.STAGING_DET_INGRS", str(staging_dir / "det_ingrs")),
        patch(f"{_CFG}.STAGING_NUTR",      str(staging_dir / "nutrition")),
        patch(f"{_CFG}.STAGING_KAGGLE",    str(staging_dir / "kaggle")),
    )


def _patch_outputs(output_dir: Path):
    """Patch les 3 constantes de chemin de sortie vers le répertoire temporaire."""
    return (
        patch(f"{_CFG}.OUT_RECIPES_MAIN",      str(output_dir / "recipes_main")),
        patch(f"{_CFG}.OUT_INGREDIENTS_INDEX", str(output_dir / "ingredients_index")),
        patch(f"{_CFG}.OUT_NUTRITION_DETAIL",  str(output_dir / "recipes_nutrition_detail")),
    )


class TestAssemble:
    def test_returns_all_three_recipes(self, spark: SparkSession, staging_dir: Path) -> None:
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            assert assemble(spark).count() == 3

    def test_required_columns_present(self, spark: SparkSession, staging_dir: Path) -> None:
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            cols = set(assemble(spark).columns)
        expected = {
            "id", "title", "title_norm", "url",
            "instructions_text", "ingredients_raw", "n_steps",
            "image_url", "image_urls", "has_image",
            "ingredients_validated", "n_ingredients_validated",
            "mit_energy_kcal", "fat_g", "protein_g",
            "cook_minutes", "description", "tags", "kaggle_energy_kcal",
        }
        assert expected.issubset(cols)

    def test_ingredients_raw_is_array(self, spark: SparkSession, staging_dir: Path) -> None:
        """from_json doit avoir converti la string JSON en ArrayType."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            field = next(
                f for f in assemble(spark).schema.fields if f.name == "ingredients_raw"
            )
        assert isinstance(field.dataType, ArrayType)

    def test_left_join_keeps_recipe_without_image(self, spark: SparkSession, staging_dir: Path) -> None:
        """cde333 n'a pas de ligne dans layer2 → doit quand même apparaître."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            rows = assemble(spark).filter("id = 'cde333'").collect()
        assert len(rows) == 1
        assert rows[0]["image_url"] is None

    def test_left_join_keeps_recipe_without_kaggle(self, spark: SparkSession, staging_dir: Path) -> None:
        """bcd222 n'a pas de match Kaggle → cook_minutes null."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            row = assemble(spark).filter("id = 'bcd222'").collect()[0]
        assert row["cook_minutes"] is None

    def test_kaggle_join_enriches_matching_recipe(self, spark: SparkSession, staging_dir: Path) -> None:
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            row = assemble(spark).filter("id = 'abc111'").collect()[0]
        assert row["cook_minutes"]        == 45
        assert row["kaggle_energy_kcal"]  == pytest.approx(380.0)

    def test_energy_kcal_renamed_to_mit(self, spark: SparkSession, staging_dir: Path) -> None:
        """energy_kcal doit être renommé mit_energy_kcal — sinon le Nutri-Score est cassé."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            cols = assemble(spark).columns
        assert "mit_energy_kcal" in cols
        assert "energy_kcal"     not in cols

    def test_no_duplicates_on_id(self, spark: SparkSession, staging_dir: Path) -> None:
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            df = assemble(spark)
        assert df.count() == df.select("id").distinct().count()


class TestWriteFinalTables:
    @pytest.fixture(scope="class")
    def output_dir(
        self,
        spark: SparkSession,
        staging_dir: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> Path:
        out      = tmp_path_factory.mktemp("output")
        staging  = _patch_staging(staging_dir)
        outputs  = _patch_outputs(out)
        with (staging[0], staging[1], staging[2], staging[3], staging[4],
              outputs[0], outputs[1], outputs[2]):
            write_final_tables(assemble(spark))
        return out

    def test_recipes_main_written(self, spark: SparkSession, output_dir: Path) -> None:
        assert spark.read.parquet(str(output_dir / "recipes_main")).count() == 3

    def test_no_null_recipe_id(self, spark: SparkSession, output_dir: Path) -> None:
        df = spark.read.parquet(str(output_dir / "recipes_main"))
        assert df.filter(df.recipe_id.isNull()).count() == 0

    def test_ingredients_index_written(self, spark: SparkSession, output_dir: Path) -> None:
        assert spark.read.parquet(str(output_dir / "ingredients_index")).count() > 0

    def test_nutrition_detail_written(self, spark: SparkSession, output_dir: Path) -> None:
        assert spark.read.parquet(str(output_dir / "recipes_nutrition_detail")).count() > 0