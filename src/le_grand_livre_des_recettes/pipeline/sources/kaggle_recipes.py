"""
Source dlt — dataset Food.com / Kaggle (RAW_recipes.csv).

Lecture ligne par ligne avec ``csv.DictReader`` — aucun chargement en mémoire.
Le dédoublonnage sur ``title_norm`` est effectué ici plutôt qu'en Phase 2 pour
éviter de propager des doublons inutiles dans le staging.
"""

from __future__ import annotations

import csv
import re
import time

import dlt

from le_grand_livre_des_recettes.pipeline import config as cfg
from le_grand_livre_des_recettes.pipeline.sources._utils import log_progress, normalize_title


def _parse_list_str(raw: str) -> list[str]:
    """
    Convertit une chaîne au format ``"['tag1', 'tag2']"`` en liste Python.

    Parameters
    ----------
    raw:
        Chaîne brute issue du CSV Kaggle (colonne ``tags`` ou similaire).
    """
    if not raw:
        return []
    cleaned = re.sub(r"[\[\]'\"]", "", raw)
    return [t.strip() for t in re.split(r",\s*", cleaned) if t.strip()]


def _parse_nutrition(raw: str) -> float | None:
    """
    Extrait la première valeur (kcal/portion) de la chaîne nutritionnelle Kaggle.

    Le format Kaggle est ``"[kcal, fat, sugar, sodium, protein, saturated, carbs]"``.
    Seule la première valeur (calories) est utilisée.

    Parameters
    ----------
    raw:
        Chaîne brute de la colonne ``nutrition``.
    """
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
def kaggle_resource() -> None:
    """
    Lit RAW_recipes.csv ligne par ligne et yield des dicts normalisés.

    Le dédoublonnage sur ``title_norm`` est effectué en mémoire via un set.
    Pour des fichiers dépassant plusieurs millions de lignes, envisager
    un filtre basé sur un Bloom filter externe.

    Yields
    ------
    dict
        Champs : kaggle_id, cook_minutes, n_steps, description, tags (list[str]),
        title_norm, kaggle_energy_kcal (kcal/portion — différent du MIT kcal/100g).
    """
    print(f"  → kaggle : lecture de {cfg.RAW_CSV_PATH} ...", flush=True)
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
                "kaggle_id":          row.get("id"),
                "cook_minutes":       int(row["minutes"]) if row.get("minutes", "").isdigit() else None,
                "n_steps":            int(row["n_steps"]) if row.get("n_steps", "").isdigit() else None,
                "description":        row.get("description") or None,
                "tags":               _parse_list_str(row.get("tags", "")),
                "title_norm":         title_norm,
                "kaggle_energy_kcal": _parse_nutrition(row.get("nutrition", "")),
            }
            count += 1
            log_progress("kaggle", count, t0)

    print(f"  ✅ kaggle : {count:,} recettes en {time.perf_counter() - t0:.1f}s", flush=True)


@dlt.source(name="kaggle_recipes")
def kaggle_source():
    """Source dlt pour le dataset Kaggle Food.com."""
    return kaggle_resource()
