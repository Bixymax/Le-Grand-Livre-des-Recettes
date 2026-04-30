"""
Tests d'intégration du pipeline PySpark.

Utilise les fixtures JSON/CSV présents dans tests/fixtures/ pour
valider chaque étape du pipeline sans avoir besoin des vrais fichiers.

Toutes les sessions Spark tournent en mode local[*].
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Session Spark partagée entre tous les tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession locale pour les tests (mode local[*])."""
    return SparkSession.builder \
        .appName("recipes_pipeline_tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def tmp_dirs():
    """Crée des dossiers temporaires pour staging et outputs."""
    tmp = Path(tempfile.mkdtemp())
    staging = tmp / "staging"
    outputs = tmp / "outputs" / "parquets"
    staging.mkdir(parents=True)
    outputs.mkdir(parents=True)
    yield {"staging": staging, "outputs": outputs, "root": tmp}
    shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests des sources
# ---------------------------------------------------------------------------

class TestMitRecipesSources:

    def test_read_layer1_schema(self, spark, fixtures_dir, tmp_dirs):
        """Vérifie que layer1 produit les colonnes attendues."""
        from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import read_layer1

        with patch("src.le_grand_livre_des_recettes.pipeline.config.LAYER1_PATH",
                   fixtures_dir / "layer1.json"):
            df = read_layer1(spark)

        cols = set(df.columns)
        assert "id" in cols
        assert "title" in cols
        assert "title_norm" in cols
        assert "instructions_text" in cols
        assert "ingredients_raw" in cols
        assert "n_steps" in cols
        # Colonnes brutes supprimées
        assert "instructions" not in cols
        assert "ingredients" not in cols

    def test_read_layer1_title_norm(self, spark, fixtures_dir):
        """Vérifie la normalisation du titre."""
        from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import read_layer1

        with patch("src.le_grand_livre_des_recettes.pipeline.config.LAYER1_PATH",
                   fixtures_dir / "layer1.json"):
            df = read_layer1(spark)

        row = df.filter(df.id == "abc111").collect()[0]
        assert row["title_norm"] == "chocolate cake"
        assert row["title"] == "chocolate cake"

    def test_read_layer1_instructions_text(self, spark, fixtures_dir):
        """Vérifie la concaténation des étapes."""
        from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import read_layer1

        with patch("src.le_grand_livre_des_recettes.pipeline.config.LAYER1_PATH",
                   fixtures_dir / "layer1.json"):
            df = read_layer1(spark)

        row = df.filter(df.id == "abc111").collect()[0]
        assert "Preheat oven to 350F." in row["instructions_text"]
        assert " | " in row["instructions_text"]

    def test_read_layer1_count(self, spark, fixtures_dir):
        """Vérifie le nombre de recettes."""
        from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import read_layer1

        with patch("src.le_grand_livre_des_recettes.pipeline.config.LAYER1_PATH",
                   fixtures_dir / "layer1.json"):
            df = read_layer1(spark)

        assert df.count() == 3

    def test_read_det_ingrs_filter(self, spark, fixtures_dir):
        """Vérifie que seuls les ingrédients valides sont conservés."""
        from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import read_det_ingrs

        with patch("src.le_grand_livre_des_recettes.pipeline.config.DET_INGRS_PATH",
                   fixtures_dir / "det_ingrs.json"):
            df = read_det_ingrs(spark)

        # bcd222 : valid = [true, true, false] → 2 ingrédients validés
        row = df.filter(df.id == "bcd222").collect()[0]
        assert row["n_ingredients_validated"] == 2
        assert "tomatoes" not in row["ingredients_validated"]
        assert "pasta" in row["ingredients_validated"]

    def test_read_layer2_image_url(self, spark, fixtures_dir):
        """Vérifie l'extraction de la première image."""
        from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import read_layer2

        with patch("src.le_grand_livre_des_recettes.pipeline.config.LAYER2_PATH",
                   fixtures_dir / "layer2+.json"):
            df = read_layer2(spark)

        assert df.count() > 0
        row = df.collect()[0]
        assert "image_url" in df.columns
        assert "has_image" in df.columns


class TestKaggleSource:

    def test_read_kaggle_schema(self, spark, fixtures_dir):
        """Vérifie les colonnes produites par la source Kaggle."""
        from src.le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import read_kaggle

        with patch("src.le_grand_livre_des_recettes.pipeline.config.RAW_CSV_PATH",
                   fixtures_dir / "RAW_recipes.csv"):
            df = read_kaggle(spark)

        cols = set(df.columns)
        assert "title_norm" in cols
        assert "cook_minutes" in cols
        assert "tags" in cols
        assert "kaggle_energy_kcal" in cols
        assert "tags_raw" not in cols

    def test_read_kaggle_dedup(self, spark, fixtures_dir):
        """Vérifie le dédoublonnage sur title_norm."""
        from src.le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import read_kaggle

        with patch("src.le_grand_livre_des_recettes.pipeline.config.RAW_CSV_PATH",
                   fixtures_dir / "RAW_recipes.csv"):
            df = read_kaggle(spark)

        total  = df.count()
        unique = df.select("title_norm").distinct().count()
        assert total == unique


# ---------------------------------------------------------------------------
# Tests des transformateurs
# ---------------------------------------------------------------------------

class TestNutriScore:

    def test_nutri_score_thresholds(self, spark):
        """Vérifie les seuils du Nutri-Score."""
        from pyspark.sql.types import FloatType, StructField, StructType
        from src.le_grand_livre_des_recettes.pipeline.transformers.enrich import _nutri_score_col

        schema = StructType([StructField("kcal", FloatType(), True)])
        data   = [(50.0,), (100.0,), (200.0,), (300.0,), (450.0,), (None,)]
        df     = spark.createDataFrame(data, schema)
        result = df.withColumn("score", _nutri_score_col("kcal")).collect()

        scores = {row["kcal"]: row["score"] for row in result}
        assert scores[50.0]  == "A"
        assert scores[100.0] == "B"
        assert scores[200.0] == "C"
        assert scores[300.0] == "D"
        assert scores[450.0] == "E"
        assert scores[None]  is None

    def test_cook_time_category(self, spark):
        """Vérifie les catégories de temps de cuisson."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, StructField, StructType

        schema = StructType([StructField("minutes", IntegerType(), True)])
        data   = [(20,), (45,), (90,), (None,)]
        df     = spark.createDataFrame(data, schema)
        result = (
            df.withColumn("cat",
                F.when(F.col("minutes") <= 30, F.lit("rapide"))
                 .when(F.col("minutes") <= 60, F.lit("moyen"))
                 .when(F.col("minutes").isNotNull(), F.lit("long"))
                 .otherwise(F.lit("inconnu"))
            )
            .collect()
        )
        cats = {row["minutes"]: row["cat"] for row in result}
        assert cats[20]   == "rapide"
        assert cats[45]   == "moyen"
        assert cats[90]   == "long"
        assert cats[None] == "inconnu"


class TestBuildIngredientIndex:

    def test_explode_ingredients(self, spark):
        """Vérifie l'explosion des ingrédients en lignes distinctes."""
        from pyspark.sql.types import ArrayType, StringType, StructField, StructType
        from src.le_grand_livre_des_recettes.pipeline.transformers.enrich import build_ingredients_index

        schema = StructType([
            StructField("recipe_id",          StringType(), True),
            StructField("title",              StringType(), True),
            StructField("nutri_score",        StringType(), True),
            StructField("image_url",          StringType(), True),
            StructField("cook_time_category", StringType(), True),
            StructField("mit_energy_kcal",    StringType(), True),  # ignoré
            StructField("ingredients_validated", ArrayType(StringType()), True),
        ])
        data = [("r1", "Cake", "B", None, "rapide", None, ["flour", "sugar", "eggs"])]
        df   = spark.createDataFrame(data, schema)
        idx  = build_ingredients_index(df)

        assert idx.count() == 3
        ingrs = {r["ingredient"] for r in idx.collect()}
        assert ingrs == {"flour", "sugar", "eggs"}

    def test_filters_empty_ingredients(self, spark):
        """Vérifie que les ingrédients vides ou null sont filtrés."""
        from pyspark.sql.types import ArrayType, StringType, StructField, StructType
        from src.le_grand_livre_des_recettes.pipeline.transformers.enrich import build_ingredients_index

        schema = StructType([
            StructField("recipe_id",          StringType(), True),
            StructField("title",              StringType(), True),
            StructField("nutri_score",        StringType(), True),
            StructField("image_url",          StringType(), True),
            StructField("cook_time_category", StringType(), True),
            StructField("mit_energy_kcal",    StringType(), True),
            StructField("ingredients_validated", ArrayType(StringType()), True),
        ])
        data = [("r1", "Cake", None, None, "inconnu", None, ["flour", "", None, "  "])]
        df   = spark.createDataFrame(data, schema)
        idx  = build_ingredients_index(df)

        ingrs = {r["ingredient"] for r in idx.collect()}
        assert "" not in ingrs
        assert None not in ingrs
        assert "flour" in ingrs
