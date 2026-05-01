"""
Fixtures partagées.

``staging_dir`` génère les 5 tables Parquet staging une seule fois (scope session)
en écrivant directement avec Spark, reproduisant ce que dlt produit :
les colonnes de type array sont sérialisées en JSON string.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("recipes_tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def staging_dir(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Crée les 5 tables Parquet staging dans un répertoire temporaire.
    Reproduit la structure écrite par dlt (arrays = JSON strings).
    """
    dst = tmp_path_factory.mktemp("staging")

    # layer1
    spark.createDataFrame(
        [
            ("abc111", "chocolate cake",  "http://example.com/choc",  "train",
             "Preheat oven to 350F. | Mix ingredients. | Bake 30 minutes.",
             json.dumps(["flour", "sugar", "eggs", "cocoa powder"]), 3, "chocolate cake"),
            ("bcd222", "pasta bolognese", "http://example.com/pasta", "train",
             "Boil pasta. | Make sauce.",
             json.dumps(["pasta", "beef", "tomatoes"]), 2, "pasta bolognese"),
            ("cde333", "apple pie",       "http://example.com/pie",   "test",
             "Make the crust. | Add filling.",
             json.dumps(["apples", "pie crust", "cinnamon"]), 2, "apple pie"),
        ],
        StructType([
            StructField("id",                StringType()),
            StructField("title",             StringType()),
            StructField("url",               StringType()),
            StructField("partition",         StringType()),
            StructField("instructions_text", StringType()),
            StructField("ingredients_raw",   StringType()),
            StructField("n_steps",           IntegerType()),
            StructField("title_norm",        StringType()),
        ]),
    ).write.parquet(str(dst / "layer1"))

    # layer2 — seulement abc111 et bcd222 ont des images
    spark.createDataFrame(
        [
            ("abc111", "http://img.example.com/choc1.jpg",
             json.dumps(["http://img.example.com/choc1.jpg", "http://img.example.com/choc2.jpg"]),
             True),
            ("bcd222", "http://img.example.com/pasta.jpg",
             json.dumps(["http://img.example.com/pasta.jpg"]),
             True),
        ],
        StructType([
            StructField("id",         StringType()),
            StructField("image_url",  StringType()),
            StructField("image_urls", StringType()),
            StructField("has_image",  BooleanType()),
        ]),
    ).write.parquet(str(dst / "layer2"))

    # det_ingrs — tomatoes (bcd222) et pie crust (cde333) ont valid=False
    spark.createDataFrame(
        [
            ("abc111", json.dumps(["flour", "sugar", "eggs", "cocoa powder"]), 4),
            ("bcd222", json.dumps(["pasta", "beef"]),                          2),
            ("cde333", json.dumps(["apples", "cinnamon"]),                     2),
        ],
        StructType([
            StructField("id",                      StringType()),
            StructField("ingredients_validated",   StringType()),
            StructField("n_ingredients_validated", IntegerType()),
        ]),
    ).write.parquet(str(dst / "det_ingrs"))

    # nutrition — chocolate cake: 350 kcal/100g → D | apple pie: 237 kcal/100g → C
    spark.createDataFrame(
        [
            ("chocolate cake", "chocolate cake", 350.0, 15.0, 5.0, 0.5, 7.0, 30.0),
            ("apple pie",      "apple pie",      237.0, 11.0, 2.5, 0.3, 4.0, 22.0),
        ],
        StructType([
            StructField("title",       StringType()),
            StructField("title_norm",  StringType()),
            StructField("energy_kcal", DoubleType()),
            StructField("fat_g",       DoubleType()),
            StructField("protein_g",   DoubleType()),
            StructField("salt_g",      DoubleType()),
            StructField("saturates_g", DoubleType()),
            StructField("sugars_g",    DoubleType()),
        ]),
    ).write.parquet(str(dst / "nutrition"))

    # kaggle — seul chocolate cake a une correspondance
    # kaggle_energy_kcal = 380 kcal/PORTION ≠ mit_energy_kcal (kcal/100g)
    spark.createDataFrame(
        [("1", 45, 4, "A rich moist chocolate cake",
          json.dumps(["dessert", "baking", "chocolate"]),
          "chocolate cake", 380.0)],
        StructType([
            StructField("kaggle_id",          StringType()),
            StructField("cook_minutes",       IntegerType()),
            StructField("n_steps",            IntegerType()),
            StructField("description",        StringType()),
            StructField("tags",               StringType()),
            StructField("title_norm",         StringType()),
            StructField("kaggle_energy_kcal", DoubleType()),
        ]),
    ).write.parquet(str(dst / "kaggle"))

    return dst