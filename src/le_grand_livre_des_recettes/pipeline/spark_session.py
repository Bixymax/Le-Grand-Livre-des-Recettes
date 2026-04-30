"""
Factory de SparkSession pour le pipeline recipes.

La session est configurée pour fonctionner dans trois contextes :
  1. Cluster Docker Spark  → master = spark://spark-master:7077
  2. Session Jupyter déjà active sur le master node → getOrCreate() la récupère
  3. Mode local (tests / dev machine sans Docker) → master = local[*]

Priorité du master URL :
  argument explicite > variable d'env SPARK_MASTER_URL > local[*]
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
    app_name : str
        Nom visible dans l'UI Spark (localhost:8080 / history server).
    master : str | None
        URL du master Spark. Si None, utilise la variable d'env
        SPARK_MASTER_URL, ou "local[*]" en dernier recours.

    Returns
    -------
    SparkSession
    """
    resolved_master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(resolved_master)
        # Sérialisation Kryo : plus rapide que Java par défaut
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Désactive le broadcast auto pour les grands datasets
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        # Shuffle partitions : 8 correspond au N_PARTITIONS du notebook Databricks
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # Niveau de log : WARNING pour éviter le bruit dans les notebooks
    spark.sparkContext.setLogLevel("WARN")

    return spark
