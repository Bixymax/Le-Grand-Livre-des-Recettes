"""
Source dlt pour le dataset Food.com / Kaggle (RAW_recipes.csv).

Ce fichier CSV contient des colonnes `tags` et `nutrition` stockées
comme des chaînes de caractères Python (ex: "['tag1', 'tag2']").
La normalisation est faite ici, en Python pur, avant le yield.

ATTENTION unité énergie :
  `kaggle_energy_kcal` est en kcal/PORTION (premier élément du champ
  `nutrition` de Food.com), pas en kcal/100g.
  Elle ne doit PAS être utilisée pour le Nutri-Score — c'est uniquement
  une valeur déclarative utile à l'affichage et à la jointure.
"""

import csv
import ast
import re
from pathlib import Path
from typing import Iterator

import dlt

from src.le_grand_livre_des_recettes.pipeline.models.recipes import KaggleStaging


@dlt.source(name="kaggle_recipes")
def kaggle_recipes_source(data_dir: str = dlt.config.value):
    return (kaggle_raw(data_dir),)


@dlt.resource(
    name="raw_kaggle",
    write_disposition="replace",
    schema_contract={"columns": "evolve"},
)
def kaggle_raw(data_dir: str) -> Iterator[KaggleStaging]:
    """
    Lit RAW_recipes.csv et normalise les colonnes complexes.
    Yield des instances KaggleStaging — dlt appelle .model_dump() en interne.
    Produit une `title_norm` identique à celle du pipeline Spark
    pour permettre la jointure SQL en aval.
    """
    path = Path(data_dir) / "RAW_recipes.csv"
    if not path.exists():
        raise FileNotFoundError(f"RAW_recipes.csv introuvable dans {data_dir}")

    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            title: str = row.get("name", "")
            title_norm: str = _normalize_title(title)

            if not title_norm:
                continue

            tags: list[str] = _parse_python_list(row.get("tags", ""))
            nutrition_raw: list[float] = _parse_nutrition(row.get("nutrition", ""))

            yield KaggleStaging(
                name=title,
                title_norm=title_norm,
                minutes=_safe_int(row.get("minutes")),
                n_steps=_safe_int(row.get("n_steps")),
                n_ingredients=_safe_int(row.get("n_ingredients")),
                description=row.get("description") or None,
                tags=tags,
                # Premier élément = kcal/portion (convention Food.com)
                # ≠ kcal/100g — NE PAS utiliser pour le Nutri-Score
                kaggle_energy_kcal=nutrition_raw[0] if nutrition_raw else None,
            )


# ---------------------------------------------------------------------------
# Helpers — logique de normalisation extraite du pipeline Spark original
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^a-zA-Z0-9 ]")


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
    for item in raw.strip("[]").split(","):
        try:
            result.append(float(item))
        except ValueError:
            pass

    return result


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
