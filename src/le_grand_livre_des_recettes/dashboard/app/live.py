"""
Couche "live" du dashboard — agrège les recettes batch (Delta -> DuckDB) avec
les nouvelles recettes du stream Kafka (Delta `recipes_stream`, lu via
`delta_scan`).

Un seul delta_scan par cycle : les données sont chargées dans une table
temporaire DuckDB, puis toutes les requêtes KPI/log s'exécutent dessus.
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

_EMPTY: dict = {
    "stream_count": 0,
    "with_image": 0,
    "with_nutrition": 0,
    "last_event_ts": None,
    "avg_kcal": None,
    "recent_events": [],
}


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
    con = duckdb.connect(":memory:")
    con.execute("INSTALL delta; LOAD delta;")
    return con


def fetch_all_stream_data(n_recent: int = 10) -> dict:
    """
    Charge la table Delta en une seule passe dans une table temporaire,
    puis extrait KPIs et derniers événements en mémoire.
    """
    if not stream_exists():
        return dict(_EMPTY)

    con = _live_connection()
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _stream AS SELECT * FROM delta_scan('{STREAM_DELTA_PATH}')"
    )

    kpi_row = con.execute("""
        SELECT
            COUNT(*)                                                              AS stream_count,
            COUNT(*) FILTER (WHERE has_image)                                     AS with_image,
            COUNT(*) FILTER (WHERE mit_energy_kcal IS NOT NULL)                   AS with_nutrition,
            MAX(event_ts)                                                         AS last_event_ts,
            ROUND(AVG(mit_energy_kcal) FILTER (WHERE mit_energy_kcal IS NOT NULL)) AS avg_kcal
        FROM _stream
    """).fetchone()

    if kpi_row is None:
        return dict(_EMPTY)

    recent_rows = con.execute(f"""
        SELECT title, nutri_score, cook_time_category, event_ts
        FROM _stream
        WHERE event_ts IS NOT NULL
        ORDER BY event_ts DESC
        LIMIT {n_recent}
    """).fetchall()

    return {
        "stream_count":  int(kpi_row[0] or 0),
        "with_image":    int(kpi_row[1] or 0),
        "with_nutrition": int(kpi_row[2] or 0),
        "last_event_ts": kpi_row[3],
        "avg_kcal":      kpi_row[4],
        "recent_events": [
            {
                "title":             row[0] or "—",
                "nutri_score":       row[1] or "?",
                "cook_time_category": row[2] or "inconnu",
                "event_ts":          row[3],
            }
            for row in recent_rows
        ],
    }


def fetch_live_kpis(
    *,
    batch_total: int,
    batch_with_image: int,
    batch_with_nutrition: int,
    stream_data: dict | None = None,
) -> LiveKpis:
    """Combine KPIs batch (statiques) et flux (dynamiques) pour affichage."""
    s = stream_data if stream_data is not None else fetch_all_stream_data()
    return {
        "total":          batch_total    + s["stream_count"],
        "with_image":     batch_with_image + s["with_image"],
        "with_nutrition": batch_with_nutrition + s["with_nutrition"],
        "stream_count":   s["stream_count"],
    }