"""Tests unitaires — expressions Spark de enrich.py."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType,
    StringType, StructField, StructType, BooleanType,
)

from le_grand_livre_des_recettes.pipeline.transformers.enrich import (
    _enrich,
    _nutri_score_col,
    build_ingredients_index,
    build_nutrition_detail,
)


class TestNutriScoreCol:
    """
    _nutri_score_col est une expression Spark — on doit démarrer une session
    pour l'évaluer. Les seuils doivent rester en sync avec la doc du pipeline.
    """

    def test_thresholds(self, spark: SparkSession) -> None:
        schema = StructType([StructField("kcal", DoubleType())])
        rows   = [(50.0,), (80.0,), (160.0,), (270.0,), (400.0,), (None,)]
        result = (
            spark.createDataFrame(rows, schema)
            .withColumn("score", _nutri_score_col("kcal"))
            .collect()
        )
        scores = {r["kcal"]: r["score"] for r in result}
        assert scores[50.0]  == "A"
        assert scores[80.0]  == "B"
        assert scores[160.0] == "C"
        assert scores[270.0] == "D"
        assert scores[400.0] == "E"
        assert scores[None]  is None


class TestEnrich:
    # Schéma minimal reproduisant la sortie de assemble()
    _SCHEMA = StructType([
        StructField("id", StringType()),
        StructField("ingredients_validated", ArrayType(StringType())),
        StructField("cook_minutes", IntegerType()),
        StructField("has_image", BooleanType())
    ])

    def test_n_ingredients_null_becomes_zero(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("r1", None, None, None)], self._SCHEMA)
        assert _enrich(df).collect()[0]["n_ingredients_validated"] == 0

    def test_n_ingredients_counted(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("r1", ["flour", "sugar", "eggs"], 30, None)], self._SCHEMA
        )
        assert _enrich(df).collect()[0]["n_ingredients_validated"] == 3

    def test_cook_time_categories(self, spark: SparkSession) -> None:
        rows = [
            ("r1", None, 20,   None),
            ("r2", None, 45,   None),
            ("r3", None, 90,   None),
            ("r4", None, None, None),
        ]
        result = {
            r["id"]: r["cook_time_category"]
            for r in _enrich(spark.createDataFrame(rows, self._SCHEMA)).collect()
        }
        assert result == {"r1": "rapide", "r2": "moyen", "r3": "long", "r4": "inconnu"}

    def test_has_image_null_becomes_false(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("r1", None, None, None)], self._SCHEMA)
        assert _enrich(df).collect()[0]["has_image"] is False


class TestBuildIngredientsIndex:
    _SCHEMA = StructType([
        StructField("recipe_id",             StringType()),
        StructField("title",                 StringType()),
        StructField("nutri_score",           StringType()),
        StructField("image_url",             StringType()),
        StructField("cook_time_category",    StringType()),
        StructField("ingredients_validated", ArrayType(StringType())),
    ])

    def test_one_row_per_ingredient(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("r1", "Cake", "B", None, "rapide", ["flour", "sugar", "eggs"])],
            self._SCHEMA,
        )
        assert build_ingredients_index(df).count() == 3

    def test_ingredient_lowercased_and_trimmed(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("r1", "Cake", None, None, "rapide", ["  FLOUR  "])],
            self._SCHEMA,
        )
        assert build_ingredients_index(df).collect()[0]["ingredient"] == "flour"

    def test_empty_and_null_filtered(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("r1", "Cake", None, None, "inconnu", ["flour", "", None, "  "])],
            self._SCHEMA,
        )
        ingrs = {r["ingredient"] for r in build_ingredients_index(df).collect()}
        assert ingrs == {"flour"}

    def test_null_array_produces_no_rows(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("r1", "Cake", None, None, "inconnu", None)],
            self._SCHEMA,
        )
        assert build_ingredients_index(df).count() == 0


class TestBuildNutritionDetail:
    _SCHEMA = StructType([
        StructField("id",          StringType()),
        StructField("fat_g",       DoubleType()),
        StructField("protein_g",   DoubleType()),
        StructField("salt_g",      DoubleType()),
        StructField("saturates_g", DoubleType()),
        StructField("sugars_g",    DoubleType()),
    ])

    def test_keeps_row_with_at_least_one_value(self, spark: SparkSession) -> None:
        rows = [
            ("r1", 15.0, None, None, None, None),  # une valeur → conservée
            ("r2", None, None, None, None, None),  # toutes nulles → exclue
        ]
        ids = {
            r["recipe_id"]
            for r in build_nutrition_detail(spark.createDataFrame(rows, self._SCHEMA)).collect()
        }
        assert ids == {"r1"}

    def test_deduplicates_on_recipe_id(self, spark: SparkSession) -> None:
        rows = [("r1", 10.0, None, None, None, None)] * 2
        assert build_nutrition_detail(spark.createDataFrame(rows, self._SCHEMA)).count() == 1