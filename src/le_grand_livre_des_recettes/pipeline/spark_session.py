"""
Factory de SparkSession pour le pipeline recipes.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_or_create_spark(
    app_name: str = "recipes_pipeline",
    master: str | None = None,
) -> SparkSession:
    """
    Retourne une SparkSession active ou en initialise une nouvelle.

    La résolution de l'URL du master Spark s'effectue selon cet ordre de priorité :
    1. L'argument explicite `master`
    2. La variable d'environnement `SPARK_MASTER_URL`
    3. Fallback sur `local[*]` (pour le développement ou la CI locale)
    """
    resolved_master: str = master or os.environ.get("SPARK_MASTER_URL", "local[*]")

    spark: SparkSession = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "1g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark