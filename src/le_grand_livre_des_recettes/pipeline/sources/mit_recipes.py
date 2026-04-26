"""
Source dlt pour les fichiers MIT Recipe1M+.

Chaque @dlt.resource est un générateur Python pur — pas de Spark, pas de Pandas.
dlt gère lui-même le buffering, la sérialisation et l'écriture vers la destination.

Fichiers attendus dans data_dir :
  - layer1.json              (titre, URL, ingrédients bruts, instructions)
  - layer2+.json             (URLs des images)
  - det_ingrs.json           (ingrédients validés avec flag booléen)
  - recipes_with_nutritional_info.json  (macronutriments MIT)
"""

from pathlib import Path
from typing import Iterator

import dlt
import ijson.backends.yajl2_c as ijson
from dlt.sources import DltResource


@dlt.source(name="mit_recipes")
def mit_recipes_source(
        data_dir: str = dlt.config.value,
) -> tuple[DltResource, ...]:
    """
    Groupe les 4 ressources MIT en une seule source cohérente.
    dlt les charge en parallèle vers des tables staging distinctes.
    """
    return (
        layer1(data_dir),
        layer2(data_dir),
        det_ingrs(data_dir),
        nutrition(data_dir),
    )


# ---------------------------------------------------------------------------
# Ressources individuelles
# ---------------------------------------------------------------------------

@dlt.resource(
    name="raw_layer1",
    parallelized=True,
    write_disposition="replace",  # full reload — sera "append" en streaming
    columns={
        "id": {"data_type": "text", "nullable": False},
        "title": {"data_type": "text"},
        "url": {"data_type": "text"},
        "partition": {"data_type": "text"},
    },
)
def layer1(data_dir: str) -> Iterator[dict]:
    """
    Extrait les recettes brutes de layer1.json via ijson en streaming.
    Normalise inline les champs que dlt ne peut pas inférer correctement
    (arrays imbriqués) avant de yielder.
    """
    path = Path(data_dir) / "layer1.json"
    if not path.exists():
        raise FileNotFoundError(f"layer1.json introuvable dans {data_dir}")

    # ijson requiert une lecture binaire ("rb")
    with open(path, "rb") as f:
        # "item" permet d'itérer sur chaque objet du tableau racine JSON
        for record in ijson.items(f, 'item'):
            # Extraction des textes depuis les structures imbriquées
            ingredients_raw: list[str] = [
                ing.get("text", "") for ing in record.get("ingredients", [])
            ]
            instructions_parts: list[str] = [
                step.get("text", "") for step in record.get("instructions", [])
            ]

            yield {
                "id": record["id"],
                "title": record.get("title", ""),
                "url": record.get("url"),
                "partition": record.get("partition"),
                "ingredients_raw": ingredients_raw,
                "n_steps": len(instructions_parts),
                "instructions_text": " | ".join(filter(None, instructions_parts)),
            }


@dlt.resource(
    name="raw_layer2",
    write_disposition="replace",
    columns={"id": {"data_type": "text", "nullable": False}},
)
def layer2(data_dir: str) -> Iterator[dict]:
    """Extrait les URLs d'images depuis layer2+.json via ijson en streaming."""
    path = Path(data_dir) / "layer2+.json"
    if not path.exists():
        return  # layer2 est optionnel

    with open(path, "rb") as f:
        for record in ijson.items(f, 'item'):
            images: list[dict] = record.get("images", [])
            image_urls: list[str] = [
                img.get("url", "") for img in images if img.get("url")
            ]

            yield {
                "id": record["id"],
                "image_urls": image_urls,
                "image_url": image_urls[0] if image_urls else None,
                "has_image": len(image_urls) > 0,
            }


@dlt.resource(
    name="raw_det_ingrs",
    write_disposition="replace",
    columns={"id": {"data_type": "text", "nullable": False}},
)
def det_ingrs(data_dir: str) -> Iterator[dict]:
    """
    Extrait les ingrédients validés depuis det_ingrs.json via ijson en streaming.
    Logique équivalente au arrays_zip + F.filter de Spark, en Python pur.
    """
    path = Path(data_dir) / "det_ingrs.json"
    if not path.exists():
        return

    with open(path, "rb") as f:
        for record in ijson.items(f, 'item'):
            # Reconstruction de la logique arrays_zip + filter sans Spark
            ingredients: list[dict] = record.get("ingredients", [])
            validated: list[str] = [
                ing.get("text", "").lower().strip()
                for ing in ingredients
                if ing.get("valid") is True and ing.get("text")
            ]

            yield {
                "id": record["id"],
                "ingredients_validated": validated,
                "n_ingredients_validated": len(validated),
            }


@dlt.resource(
    name="raw_nutrition",
    write_disposition="replace",
    columns={"title": {"data_type": "text", "nullable": False}},
)
def nutrition(data_dir: str) -> Iterator[dict]:
    """Extrait les valeurs nutritionnelles depuis le JSON MIT via ijson en streaming."""
    path = Path(data_dir) / "recipes_with_nutritional_info.json"
    if not path.exists():
        return

    with open(path, "rb") as f:
        for record in ijson.items(f, 'item'):
            yield {
                "title": record.get("title", ""),
                "energy": _safe_float(record.get("energy")),
                "fat": _safe_float(record.get("fat")),
                "protein": _safe_float(record.get("protein")),
                "salt": _safe_float(record.get("salt")),
                "saturates": _safe_float(record.get("saturates")),
                "sugars": _safe_float(record.get("sugars")),
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
