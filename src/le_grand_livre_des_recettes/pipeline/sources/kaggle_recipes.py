"""
Source dlt pour le dataset Food.com / Kaggle (RAW_recipes.csv).

Ce fichier CSV contient des colonnes `tags` et `nutrition` stockées
comme des chaînes de caractères Python (ex: "['tag1', 'tag2']").
La normalisation est faite ici, en Python pur, avant le yield.
"""

import csv
import ast
import re
from pathlib import Path
from typing import Iterator

import dlt


@dlt.source(name="kaggle_recipes")
def kaggle_recipes_source(data_dir: str = dlt.config.value):
    return (kaggle_raw(data_dir),)


@dlt.resource(
    name="raw_kaggle",
    write_disposition="replace",
    columns={
        "name":         {"data_type": "text", "nullable": False},
        "title_norm":   {"data_type": "text"},  # clé de jointure
        "minutes":      {"data_type": "bigint"},
        "n_steps":      {"data_type": "bigint"},
        "n_ingredients":{"data_type": "bigint"},
        "description":  {"data_type": "text"},
    },
)
def kaggle_raw(data_dir: str) -> Iterator[dict]:
    """
    Lit RAW_recipes.csv et normalise les colonnes complexes.
    Produit une `title_norm` identique à celle du pipeline Spark
    pour permettre la jointure SQL en aval.
    """
    path = Path(data_dir) / "RAW_recipes.csv"
    if not path.exists():
        raise FileNotFoundError(f"RAW_recipes.csv introuvable dans {data_dir}")

    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        seen_titles: set[str] = set()  # dropDuplicates sur title_norm

        for row in reader:
            title: str = row.get("name", "")
            title_norm: str = _normalize_title(title)

            # Dédoublonnage — équivalent du dropDuplicates(["title_norm"]) Spark
            if title_norm in seen_titles or not title_norm:
                continue
            seen_titles.add(title_norm)

            tags: list[str] = _parse_python_list(row.get("tags", ""))
            nutrition_raw: list[float] = _parse_nutrition(row.get("nutrition", ""))

            yield {
                "name":            title,
                "title_norm":      title_norm,
                "minutes":         _safe_int(row.get("minutes")),
                "n_steps":         _safe_int(row.get("n_steps")),
                "n_ingredients":   _safe_int(row.get("n_ingredients")),
                "description":     row.get("description") or None,
                "tags":            tags,
                # Premier élément = kcal (convention Food.com)
                "kaggle_energy_kcal": nutrition_raw[0] if nutrition_raw else None,
            }


# ---------------------------------------------------------------------------
# Helpers — logique de normalisation extraite du pipeline Spark original
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^a-zA-Z0-9 ]")
_LIST_CHARS_RE = re.compile(r"[\[\]']")
_BRACKETS_RE = re.compile(r"[\[\]]")

def _normalize_title(title: str) -> str:
    r"""
    Reproduit exactement :
      F.lower(F.trim(F.regexp_replace("title", r"[^a-zA-Z0-9\s]", "")))
    pour garantir la compatibilité de la clé de jointure.
    """
    return _PUNCT_RE.sub("", title).lower().strip()


def _parse_python_list(raw: str) -> list[str]:
    """
    Convertit une chaîne type "['tag1', 'tag2']" en list[str].
    Reproduit le regexp_replace + split du pipeline Spark.
    """
    try:
        return [str(x).strip() for x in ast.literal_eval(raw)]
    except Exception:
        return []


def _parse_nutrition(raw: str) -> list[float]:
    """Extrait les valeurs numériques depuis "[kcal, fat, sugar, ...]"."""
    if not raw or raw == "[]":
        return []

    result = []
    # .strip() et .split() sont des méthodes natives très rapides
    for item in raw.strip("[]").split(","):
        try:
            result.append(float(item))  # float() s'occupe de virer les espaces
        except ValueError:
            pass

    return result


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
