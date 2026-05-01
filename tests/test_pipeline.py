"""Tests d'intégration Spark pour les étapes d'assemblage et d'écriture.

Valide la logique de la fonction `assemble()` et l'écriture des tables finales
via `write_final_tables()` en utilisant des patchs de configuration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType

from le_grand_livre_des_recettes.pipeline.transformers.assemble import assemble
from le_grand_livre_des_recettes.pipeline.transformers.enrich import write_final_tables

_CFG = "le_grand_livre_des_recettes.pipeline.config"


def _patch_staging(staging_dir: Path) -> tuple[Any, ...]:
    """Génère les patchs de configuration pour les répertoires de staging."""
    return (
        patch(f"{_CFG}.STAGING_LAYER1", str(staging_dir / "layer1")),
        patch(f"{_CFG}.STAGING_LAYER2", str(staging_dir / "layer2")),
        patch(f"{_CFG}.STAGING_DET_INGRS", str(staging_dir / "det_ingrs")),
        patch(f"{_CFG}.STAGING_NUTR", str(staging_dir / "nutrition")),
        patch(f"{_CFG}.STAGING_KAGGLE", str(staging_dir / "kaggle")),
    )


def _patch_outputs(output_dir: Path) -> tuple[Any, ...]:
    """Génère les patchs de configuration pour les répertoires de sortie."""
    return (
        patch(f"{_CFG}.OUT_RECIPES_MAIN", str(output_dir / "recipes_main")),
        patch(f"{_CFG}.OUT_INGREDIENTS_INDEX", str(output_dir / "ingredients_index")),
        patch(f"{_CFG}.OUT_NUTRITION_DETAIL", str(output_dir / "recipes_nutrition_detail")),
    )


class TestAssemble:
    """Tests d'intégration dédiés à la fonction assemble()."""

    def test_returns_all_three_recipes(self, spark: SparkSession, staging_dir: Path) -> None:
        """Vérifie que la jointure conserve l'ensemble des recettes attendues."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            assert assemble(spark).count() == 3

    def test_required_columns_present(self, spark: SparkSession, staging_dir: Path) -> None:
        """S'assure de la présence de toutes les colonnes requises après assemblage."""
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
        """Valide la désérialisation de la chaîne JSON en ArrayType par from_json."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            field = next(
                f for f in assemble(spark).schema.fields if f.name == "ingredients_raw"
            )
        assert isinstance(field.dataType, ArrayType)

    def test_left_join_keeps_recipe_without_image(self, spark: SparkSession, staging_dir: Path) -> None:
        """Vérifie qu'une recette sans correspondance d'image est conservée (Left Join)."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            rows = assemble(spark).filter("id = 'cde333'").collect()

        assert len(rows) == 1
        assert rows[0]["image_url"] is None

    def test_left_join_keeps_recipe_without_kaggle(self, spark: SparkSession, staging_dir: Path) -> None:
        """Vérifie qu'une recette sans correspondance Kaggle conserve des valeurs nulles."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            row = assemble(spark).filter("id = 'bcd222'").collect()[0]

        assert row["cook_minutes"] is None

    def test_kaggle_join_enriches_matching_recipe(self, spark: SparkSession, staging_dir: Path) -> None:
        """Valide l'enrichissement des données pour les recettes présentes dans la base Kaggle."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            row = assemble(spark).filter("id = 'abc111'").collect()[0]

        assert row["cook_minutes"] == 45
        assert row["kaggle_energy_kcal"] == pytest.approx(380.0)

    def test_energy_kcal_renamed_to_mit(self, spark: SparkSession, staging_dir: Path) -> None:
        """S'assure du renommage de la colonne d'énergie MIT pour éviter les conflits."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            cols = assemble(spark).columns

        assert "mit_energy_kcal" in cols
        assert "energy_kcal" not in cols

    def test_no_duplicates_on_id(self, spark: SparkSession, staging_dir: Path) -> None:
        """Vérifie l'absence de doublons introduits par les jointures successives."""
        patches = _patch_staging(staging_dir)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            df = assemble(spark)

        assert df.count() == df.select("id").distinct().count()


class TestWriteFinalTables:
    """Tests d'intégration dédiés à l'écriture des tables finales."""

    @pytest.fixture(scope="class")
    def output_dir(
        self,
        spark: SparkSession,
        staging_dir: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> Path:
        """Fixture exécutant l'écriture des résultats dans un répertoire temporaire."""
        out = tmp_path_factory.mktemp("output")
        staging = _patch_staging(staging_dir)
        outputs = _patch_outputs(out)

        with (
            staging[0], staging[1], staging[2], staging[3], staging[4],
            outputs[0], outputs[1], outputs[2]
        ):
            write_final_tables(assemble(spark))

        return out

    def test_recipes_main_written(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que la table principale des recettes est correctement enregistrée."""
        assert spark.read.format("delta").load(str(output_dir / "recipes_main")).count() == 3

    def test_no_null_recipe_id(self, spark: SparkSession, output_dir: Path) -> None:
        """S'assure qu'aucun identifiant de recette n'est nul dans la table principale."""
        df = spark.read.format("delta").load(str(output_dir / "recipes_main"))
        assert df.filter(df.recipe_id.isNull()).count() == 0

    def test_ingredients_index_written(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que l'index des ingrédients contient des données exploitables."""
        assert spark.read.format("delta").load(str(output_dir / "ingredients_index")).count() > 0

    def test_nutrition_detail_written(self, spark: SparkSession, output_dir: Path) -> None:
        """Vérifie que les détails nutritionnels sont correctement enregistrés."""
        assert spark.read.format("delta").load(str(output_dir / "recipes_nutrition_detail")).count() > 0