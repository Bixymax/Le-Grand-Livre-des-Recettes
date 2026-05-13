"""
Producer Kafka — simule l'arrivée de nouvelles recettes en temps réel.

Lit un échantillon de recettes depuis le Delta `recipes_main` (déjà construit
par le pipeline batch) et publie chaque ligne comme événement JSON dans le
topic Kafka `recipes-stream`, à intervalle régulier.

Permet de simuler un flux temps réel sans avoir besoin d'une vraie source
externe — idéal pour démontrer Spark Structured Streaming et le rafraîchissement
du dashboard.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from pathlib import Path
from typing import Any

from le_grand_livre_des_recettes.pipeline import config as cfg


def _read_sample_recipes(n: int) -> list[dict[str, Any]]:
    """
    Charge un échantillon de recettes du Delta `recipes_main` pour servir de
    pool d'événements à émettre. Utilise pyarrow + deltalake (pas Spark) pour
    éviter de démarrer une JVM dans le producer.
    """
    from deltalake import DeltaTable

    delta_path = Path(cfg.OUT_RECIPES_MAIN)
    if not delta_path.exists():
        raise FileNotFoundError(
            f"Delta table introuvable : {delta_path}. "
            "Lance d'abord le pipeline batch (python run_pipeline.py run)."
        )

    dt = DeltaTable(str(delta_path))
    cols = [
        "recipe_id",
        "title",
        "instructions_text",
        "ingredients_validated",
        "cook_minutes",
        "image_url",
        "mit_energy_kcal",
        "tags",
    ]
    table = dt.to_pyarrow_table(columns=cols).slice(0, max(n * 4, 200))
    rows = table.to_pylist()
    random.shuffle(rows)
    return rows[:n]


def _build_event(template: dict[str, Any]) -> dict[str, Any]:
    """
    Construit un événement Kafka à partir d'une recette template.

    Génère un `recipe_id` unique pour éviter les collisions avec les recettes
    déjà présentes dans le Delta batch, et tague l'événement avec un timestamp
    d'émission pour mesurer la latence end-to-end.
    """
    return {
        "recipe_id": f"stream-{uuid.uuid4().hex[:12]}",
        "title": template.get("title") or "Recette sans titre",
        "instructions_text": template.get("instructions_text") or "",
        "ingredients_validated": template.get("ingredients_validated") or [],
        "cook_minutes": template.get("cook_minutes"),
        "image_url": template.get("image_url"),
        "mit_energy_kcal": template.get("mit_energy_kcal"),
        "tags": template.get("tags") or [],
        "event_ts_ms": int(time.time() * 1000),
    }


def run_producer(
    *,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    delay_seconds: float = 2.0,
    max_events: int | None = None,
) -> None:
    """
    Démarre le producer Kafka.

    Args:
        bootstrap_servers: URL des brokers Kafka (par défaut depuis config).
        topic: Topic Kafka cible (par défaut depuis config).
        delay_seconds: Délai entre deux envois (en secondes).
        max_events: Nombre max d'événements à émettre (None = boucle infinie).
    """
    from kafka import KafkaProducer  # import paresseux : dépendance optionnelle

    bootstrap = bootstrap_servers or cfg.KAFKA_BOOTSTRAP_SERVERS
    topic_name = topic or cfg.KAFKA_TOPIC_RECIPES

    print(f"[producer] Connexion à Kafka : {bootstrap}")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=10,
    )

    print("[producer] Chargement d'un échantillon de recettes depuis Delta...")
    pool_size = max(max_events or 1000, 200)
    pool = _read_sample_recipes(pool_size)
    print(f"[producer] {len(pool)} recettes chargées comme pool d'événements.")

    print(
        f"[producer] Émission sur topic={topic_name} "
        f"(délai={delay_seconds}s, max={max_events or '∞'})"
    )

    sent = 0
    try:
        while True:
            template = random.choice(pool)
            event = _build_event(template)
            producer.send(topic_name, key=event["recipe_id"], value=event)

            sent += 1
            if sent % 10 == 0:
                producer.flush()
                print(f"[producer] {sent} événements émis")

            if max_events is not None and sent >= max_events:
                break

            time.sleep(delay_seconds)
    except KeyboardInterrupt:
        print("\n[producer] Interruption — flush final...")
    finally:
        producer.flush()
        producer.close()
        print(f"[producer] Terminé. Total : {sent} événements.")