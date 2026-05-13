"""
Consumer Spark Structured Streaming — Kafka -> Delta Lake.

Lit en continu le topic `recipes-stream`, parse les événements JSON, applique
le même enrichissement que la phase batch (Nutri-Score, cook_time_category)
et écrit en mode append dans la table Delta `recipes_stream`.

La table Delta de sortie partage la même structure que `recipes_main` pour
permettre une vue UNION dans DuckDB côté dashboard.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from le_grand_livre_des_recettes.pipeline import config as cfg
from le_grand_livre_des_recettes.pipeline.spark_session import get_or_create_spark


EVENT_SCHEMA = StructType(
    [
        StructField("recipe_id", StringType(), nullable=False),
        StructField("title", StringType()),
        StructField("instructions_text", StringType()),
        StructField("ingredients_validated", ArrayType(StringType())),
        StructField("cook_minutes", IntegerType()),
        StructField("image_url", StringType()),
        StructField("mit_energy_kcal", StringType()),  # cast après parse
        StructField("tags", ArrayType(StringType())),
        StructField("event_ts_ms", LongType()),
    ]
)


def _nutri_score_col(kcal_col: str) -> F.Column:
    """Calcul du Nutri-Score (A–E) à partir des kcal/100g — identique au batch."""
    c = F.col(kcal_col)
    return (
        F.when(c.isNull(), F.lit(None))
        .when(c < 80, F.lit("A"))
        .when(c < 160, F.lit("B"))
        .when(c < 270, F.lit("C"))
        .when(c < 400, F.lit("D"))
        .otherwise(F.lit("E"))
    )


def _enrich_stream(df: DataFrame) -> DataFrame:
    """Applique les colonnes dérivées sur le flux."""
    return (
        df.withColumn("mit_energy_kcal", F.col("mit_energy_kcal").cast("double"))
        .withColumn(
            "n_ingredients_validated",
            F.when(
                F.col("ingredients_validated").isNotNull(),
                F.size("ingredients_validated"),
            ).otherwise(0),
        )
        .withColumn(
            "cook_time_category",
            F.when(F.col("cook_minutes") <= 30, F.lit("rapide"))
            .when(F.col("cook_minutes") <= 60, F.lit("moyen"))
            .when(F.col("cook_minutes").isNotNull(), F.lit("long"))
            .otherwise(F.lit("inconnu")),
        )
        .withColumn("nutri_score", _nutri_score_col("mit_energy_kcal"))
        .withColumn("has_image", F.col("image_url").isNotNull())
        .withColumn(
            "event_ts",
            F.to_timestamp((F.col("event_ts_ms") / 1000).cast("double")),
        )
    )


def _ensure_topic_exists(bootstrap_servers: str, topic: str) -> None:
    """Create the Kafka topic if it doesn't already exist."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        print(f"[consumer] Topic '{topic}' créé.")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def _build_streaming_session(app_name: str = "recipes_stream_consumer") -> SparkSession:
    """
    Construit une SparkSession avec le connecteur Kafka chargé via Maven.
    Le package `spark-sql-kafka` doit matcher la version Spark (3.5.5).
    """
    import os

    # Récupère/initialise la session Spark de base (Delta déjà configuré)
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 pyspark-shell",
    )
    return get_or_create_spark(app_name=app_name)


def run_consumer(
    *,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    trigger_seconds: int = 10,
    starting_offsets: str = "latest",
    await_termination: bool = True,
) -> StreamingQuery:
    """
    Démarre le consumer Spark Structured Streaming.

    Args:
        bootstrap_servers: URL des brokers Kafka.
        topic: Topic Kafka à consommer.
        trigger_seconds: Période du micro-batch Spark (en secondes).
        starting_offsets: 'latest' (par défaut) ou 'earliest'.
        await_termination: Si True, bloque jusqu'à l'arrêt du flux.

    Returns:
        Le StreamingQuery actif (utile pour les tests).
    """
    bootstrap = bootstrap_servers or cfg.KAFKA_BOOTSTRAP_SERVERS
    topic_name = topic or cfg.KAFKA_TOPIC_RECIPES

    _ensure_topic_exists(bootstrap, topic_name)
    spark = _build_streaming_session()
    print(f"[consumer] Spark prêt. Kafka : {bootstrap}, topic : {topic_name}")

    Path(cfg.STREAM_CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic_name)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("event", F.from_json("json_str", EVENT_SCHEMA))
        .select("event.*")
        .filter(F.col("recipe_id").isNotNull())
    )

    enriched = _enrich_stream(parsed)

    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(cfg.STREAM_CHECKPOINT_DIR))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start(cfg.OUT_RECIPES_STREAM)
    )

    print(
        f"[consumer] Streaming démarré -> {cfg.OUT_RECIPES_STREAM} "
        f"(trigger={trigger_seconds}s). Ctrl+C pour arrêter."
    )

    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n[consumer] Interruption — arrêt du flux...")
            query.stop()

    return query