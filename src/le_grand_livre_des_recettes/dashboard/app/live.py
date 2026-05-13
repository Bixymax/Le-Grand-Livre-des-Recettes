"""
Couche "live" du dashboard — agrège les recettes batch (Delta -> DuckDB) avec
les nouvelles recettes du stream Kafka (Delta `recipes_stream`, lu via
`delta_scan`).

L'objectif est de garder l'affichage rafraîchi toutes les 30 secondes : on ne
re-construit pas la base DuckDB à chaque tick (trop lent) ; on calcule
simplement les KPIs sur le Delta streaming en mémoire et on les ajoute aux
totaux batch précalculés au démarrage.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import TypedDict

import duckdb


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STREAM_DELTA_PATH = os.path.join(
    BASE_DIR, "..", "..", "..", "..", "data", "outputs", "delta", "recipes_stream"
)


class LiveKpis(TypedDict):
    total: int
    with_image: int
    with_nutrition: int
    stream_count: int


def stream_exists() -> bool:
    """Le Delta `recipes_stream` n'apparaît qu'après le premier flush du consumer."""
    return os.path.isdir(os.path.join(STREAM_DELTA_PATH, "_delta_log"))


@lru_cache(maxsize=1)
def _live_connection() -> duckdb.DuckDBPyConnection:
    """
    Connexion DuckDB en mémoire dédiée aux lectures Delta du flux.

    Séparée de la connexion principale (read-only sur le .duckdb persistant)
    pour éviter de polluer la session du dashboard avec l'extension delta.
    """
    con = duckdb.connect(":memory:")
    con.execute("INSTALL delta; LOAD delta;")
    return con


def fetch_stream_kpis() -> dict[str, int]:
    """
    Calcule les KPIs sur le Delta streaming uniquement.

    Retourne des zéros tant que le consumer n'a rien écrit (pas de _delta_log).
    """
    if not stream_exists():
        return {"stream_count": 0, "with_image": 0, "with_nutrition": 0}

    row = _live_connection().execute(
        f"""
        SELECT
            COUNT(*)                                              AS stream_count,
            COUNT(*) FILTER (WHERE has_image)                     AS with_image,
            COUNT(*) FILTER (WHERE mit_energy_kcal IS NOT NULL)   AS with_nutrition
        FROM delta_scan('{STREAM_DELTA_PATH}')
        """
    ).fetchone()

    return {
        "stream_count": int(row[0] or 0),
        "with_image": int(row[1] or 0),
        "with_nutrition": int(row[2] or 0),
    }


def fetch_live_kpis(
    *,
    batch_total: int,
    batch_with_image: int,
    batch_with_nutrition: int,
) -> LiveKpis:
    """
    Combine KPIs batch (statiques) et flux (dynamiques) pour affichage.
    """
    s = fetch_stream_kpis()
    return {
        "total": batch_total + s["stream_count"],
        "with_image": batch_with_image + s["with_image"],
        "with_nutrition": batch_with_nutrition + s["with_nutrition"],
        "stream_count": s["stream_count"],
    }