"""
Couche "live" du dashboard — agrège les recettes batch (Delta -> DuckDB) avec
les nouvelles recettes du stream Kafka (Delta `recipes_stream`, lu via
`delta_scan`).

Un seul delta_scan par cycle : les données sont chargées dans une table
temporaire DuckDB, puis toutes les requêtes KPI/log s'exécutent dessus.
"""

from __future__ import annotations

import os
import threading
from functools import lru_cache
from typing import TypedDict

import duckdb


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DELTA_DIR = os.path.join(BASE_DIR, "..", "..", "..", "..", "data", "outputs", "delta")
STREAM_DELTA_PATH = os.path.join(DELTA_DIR, "recipes_stream")

# Mtime-based cache: évite de rescanner Delta si aucun nouveau fichier n'est arrivé.
_delta_ready_cons: set[int] = set()
_stream_scan_mtime: float = -1.0
_stream_data_cache: dict = {}
_stream_insert_mtime: float = -1.0
_insert_lock = threading.Lock()

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


def _delta_log_mtime() -> float:
    """Retourne le mtime du _delta_log ; -1.0 si inexistant."""
    try:
        return os.path.getmtime(os.path.join(STREAM_DELTA_PATH, "_delta_log"))
    except OSError:
        return -1.0


def _ensure_delta(con: duckdb.DuckDBPyConnection) -> None:
    """Charge l'extension delta sur `con` une seule fois (identifié par id())."""
    cid = id(con)
    if cid not in _delta_ready_cons:
        try:
            con.execute("LOAD delta;")
        except Exception:
            con.execute("INSTALL delta; LOAD delta;")
        _delta_ready_cons.add(cid)


_live_lock = threading.Lock()


@lru_cache(maxsize=1)
def _live_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL delta; LOAD delta;")
    return con


def fetch_all_stream_data(n_recent: int = 10) -> dict:
    """
    Charge la table Delta en une seule passe dans une table temporaire,
    puis extrait KPIs et derniers événements en mémoire.
    Le résultat est mis en cache jusqu'au prochain changement du _delta_log.
    """
    global _stream_scan_mtime, _stream_data_cache

    if not stream_exists():
        return dict(_EMPTY)

    mtime = _delta_log_mtime()
    if mtime > 0 and mtime == _stream_scan_mtime and _stream_data_cache:
        return _stream_data_cache

    with _live_lock:
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

    result = {
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
    _stream_scan_mtime = mtime
    _stream_data_cache = result
    return result


def incremental_update_from_stream(con: duckdb.DuckDBPyConnection) -> int:
    """
    Insère dans recipes_main les recettes stream absentes du catalogue.
    Utilise la connexion existante pour éviter tout conflit de verrou DuckDB.
    Retourne le nombre de lignes insérées.
    """
    global _stream_insert_mtime

    if not stream_exists():
        return 0

    mtime = _delta_log_mtime()
    if mtime > 0 and mtime == _stream_insert_mtime:
        return 0

    with _insert_lock:
        # Re-vérifier dans le lock : un thread concurrent a peut-être déjà fait le travail
        if mtime > 0 and mtime == _stream_insert_mtime:
            return 0

        _ensure_delta(con)

        cur = con.cursor()
        stream_path = STREAM_DELTA_PATH.replace("\\", "/")

        describe = cur.execute("DESCRIBE recipes_main").fetchall()
        main_cols = [row[0] for row in describe]
        main_types = {row[0]: row[1] for row in describe}

        if not main_cols:
            return 0

        stream_col_set = {
            row[0]
            for row in cur.execute(f"DESCRIBE SELECT * FROM delta_scan('{stream_path}')").fetchall()
        }

        # Détecter un reset du Delta : si DuckDB a plus de lignes stream que le Delta, purger les stale rows
        delta_count = cur.execute(f"SELECT COUNT(*) FROM delta_scan('{stream_path}')").fetchone()[0]
        duckdb_stream_count = cur.execute("SELECT COUNT(*) FROM recipes_main WHERE recipe_id LIKE 'stream-%'").fetchone()[0]
        if duckdb_stream_count > delta_count:
            con.execute("DELETE FROM recipes_main WHERE recipe_id LIKE 'stream-%'")
            con.execute("DELETE FROM recipes_nutrition WHERE recipe_id LIKE 'stream-%'")

        select_parts = [
            f"s.{col}" if col in stream_col_set else f"NULL::{main_types[col]} AS {col}"
            for col in main_cols
        ]

        before = cur.execute("SELECT COUNT(*) FROM recipes_main").fetchone()[0]
        con.execute(f"""
            INSERT INTO recipes_main ({", ".join(main_cols)})
            SELECT {", ".join(select_parts)}
            FROM delta_scan('{STREAM_DELTA_PATH}') s
            LEFT JOIN recipes_main m ON s.recipe_id = m.recipe_id
            WHERE m.recipe_id IS NULL
        """)
        inserted = con.execute("SELECT COUNT(*) FROM recipes_main").fetchone()[0] - before

        # Insérer aussi dans recipes_nutrition si le stream contient des données nutritionnelles
        _NUTRITION_COLS = ["fat_g", "protein_g", "salt_g", "saturates_g", "sugars_g"]
        available = [c for c in _NUTRITION_COLS if c in stream_col_set]
        if inserted > 0 and available:
            nutr_select = ", ".join(
                f"s.{c}" if c in available else f"NULL AS {c}"
                for c in _NUTRITION_COLS
            )
            con.execute(f"""
                INSERT INTO recipes_nutrition (recipe_id, {", ".join(_NUTRITION_COLS)})
                SELECT s.recipe_id, {nutr_select}
                FROM delta_scan('{STREAM_DELTA_PATH}') s
                LEFT JOIN recipes_nutrition n ON s.recipe_id = n.recipe_id
                WHERE n.recipe_id IS NULL
                  AND (s.fat_g IS NOT NULL OR s.protein_g IS NOT NULL
                       OR s.salt_g IS NOT NULL OR s.saturates_g IS NOT NULL
                       OR s.sugars_g IS NOT NULL)
            """)

        _stream_insert_mtime = mtime
        return inserted


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