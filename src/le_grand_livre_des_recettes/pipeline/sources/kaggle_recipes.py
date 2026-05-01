"""
Source dlt — Dataset Food.com / Kaggle (RAW_recipes.csv).

Streaming via csv.DictReader. Le dédoublonnage sur `title_norm`
est géré en mémoire via un set pour garantir l'unicité avant staging.
"""

from __future__ import annotations

import csv
import re
import time
from typing import Iterator, Any

import dlt

from le_grand_livre_des_recettes.pipeline import config as cfg
from le_grand_livre_des_recettes.pipeline.sources._utils import log_progress, normalize_title


def _parse_list_str(raw: str) -> list[str]:
    """Parse une liste stringifiée type "['tag1', 'tag2']"."""
    if not raw:
        return []
    cleaned = re.sub(r"[\[\]'\"]", "", raw)
    return [t.strip() for t in re.split(r",\s*", cleaned) if t.strip()]


def _parse_nutrition(raw: str) -> float | None:
    """Extrait la valeur calorique (kcal/portion) de l'array stringifié Kaggle."""
    if not raw:
        return None
    cleaned = re.sub(r"[\[\]]", "", raw)
    try:
        return float(cleaned.split(",")[0].strip())
    except (ValueError, IndexError):
        return None


@dlt.resource(
    name="kaggle",
    write_disposition=cfg.DLT_WRITE_DISPOSITION,
    columns={"tags": {"data_type": "complex"}},
)
def kaggle_resource() -> Iterator[dict[str, Any]]:
    """Yield les recettes Kaggle normalisées avec dédoublonnage in-memory."""
    print(f"  [kaggle] Lecture de {cfg.RAW_CSV_PATH} ...", flush=True)
    t0 = time.perf_counter()
    seen_titles: set[str] = set()
    count = 0

    with open(cfg.RAW_CSV_PATH, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title_norm = normalize_title(row.get("name", ""))

            if not title_norm or title_norm in seen_titles:
                continue
            seen_titles.add(title_norm)

            yield {
                "kaggle_id": row.get("id"),
                "cook_minutes": int(row["minutes"]) if row.get("minutes", "").isdigit() else None,
                "n_steps": int(row["n_steps"]) if row.get("n_steps", "").isdigit() else None,
                "description": row.get("description") or None,
                "tags": _parse_list_str(row.get("tags", "")),
                "title_norm": title_norm,
                "kaggle_energy_kcal": _parse_nutrition(row.get("nutrition", "")),
            }
            count += 1
            log_progress("kaggle", count, t0)

    print(f"  [kaggle] TERMINE : {count:,} recettes en {time.perf_counter() - t0:.1f}s", flush=True)


@dlt.source(name="kaggle_recipes")
def kaggle_source():
    """Point d'entrée de la source dlt pour le dataset Food.com/Kaggle."""
    return kaggle_resource()