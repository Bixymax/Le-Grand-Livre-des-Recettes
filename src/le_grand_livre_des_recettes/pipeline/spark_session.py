"""
Factory de SparkSession pour le pipeline recipes.

Priorité du master URL :
  1. Argument explicite ``master=``
  2. Variable d'environnement ``SPARK_MASTER_URL``
  3. Fallback : ``local[*]``  (dev / CI sans cluster)
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_or_create_spark(
    app_name: str = "recipes_pipeline",
    master: str | None = None,
) -> SparkSession:
    """
    Retourne une SparkSession active (ou en crée une nouvelle).

    Parameters
    ----------
    app_name:
        Nom visible dans l'UI Spark (localhost:8080 / history server).
    master:
        URL du master Spark. Si ``None``, consulte ``SPARK_MASTER_URL``,
        puis utilise ``local[*]`` en dernier recours.
    """
    resolved_master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(resolved_master)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "1g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark