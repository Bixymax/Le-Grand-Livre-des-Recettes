"""
Source dlt pour les fichiers MIT Recipe1M+.

Chaque @dlt.resource est un générateur Python pur — pas de Spark, pas de Pandas.
dlt gère lui-même le buffering, la sérialisation et l'écriture vers la destination.

Intégration Pydantic ↔ dlt :
  - Le générateur yield des instances *Staging (ex: Layer1Staging).
  - dlt détecte les instances Pydantic et infère automatiquement le schéma
    des tables staging depuis la définition de la classe (types, nullable…).
  - `columns=Model` N'EST PAS utilisé : passer `columns=` ET yielder des
    instances Pydantic crée un double-validateur conflictuel — dlt enveloppe
    le modèle dans un `ModelExtraAllow` qui ne peut pas valider une instance
    déjà typée, levant une ResourceExtractionError.
  - `schema_contract={"columns": "evolve"}` autorise l'évolution du schéma
    entre deux runs (sinon dlt gèle le schéma après le 1er run et tout
    changement de nullable lève une DataValidationError).

Fichiers attendus dans data_dir :
  - layer1.json                          (titre, URL, ingrédients bruts, instructions)
  - layer2+.json                         (URLs des images)
  - det_ingrs.json                       (ingrédients validés avec flag booléen)
  - recipes_with_nutritional_info.json   (macronutriments MIT, kcal/100g)
"""

from pathlib import Path
from typing import Iterator

import dlt
import ijson.backends.yajl2_c as ijson
from dlt.sources import DltResource

from src.le_grand_livre_des_recettes.pipeline.models.recipes import (
    Layer1Staging,
    Layer2Staging,
    DetIngrStaging,
    NutritionStaging,
)


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
    write_disposition="replace",
    # columns= retiré : dlt infère le schéma automatiquement depuis les instances
    # Layer1Staging yieldées. Passer columns= ET yielder des instances Pydantic
    # crée un double-validateur conflictuel (contract_mode=freeze).
    schema_contract={"columns": "evolve"},
)
def layer1(data_dir: str) -> Iterator[Layer1Staging]:
    """
    Extrait les recettes brutes de layer1.json via ijson en streaming.
    Yield des instances Layer1Staging — dlt appelle .model_dump() en interne.
    """
    path = Path(data_dir) / "layer1.json"
    if not path.exists():
        raise FileNotFoundError(f"layer1.json introuvable dans {data_dir}")

    with open(path, "rb") as f:
        for record in ijson.items(f, "item"):
            ingredients_raw: list[str] = [
                ing.get("text", "") for ing in record.get("ingredients", [])
            ]
            instructions_parts: list[str] = [
                step.get("text", "") for step in record.get("instructions", [])
            ]

            yield Layer1Staging(
                id=record["id"],
                title=record.get("title", ""),
                url=record.get("url"),
                partition=record.get("partition"),
                ingredients_raw=ingredients_raw,
                n_steps=len(instructions_parts),
                instructions_text=" | ".join(filter(None, instructions_parts)),
            )


@dlt.resource(
    name="raw_layer2",
    write_disposition="replace",
    schema_contract={"columns": "evolve"},
)
def layer2(data_dir: str) -> Iterator[Layer2Staging]:
    """Extrait les URLs d'images depuis layer2+.json via ijson en streaming."""
    path = Path(data_dir) / "layer2+.json"
    if not path.exists():
        return  # layer2 est optionnel

    with open(path, "rb") as f:
        for record in ijson.items(f, "item"):
            images: list[dict] = record.get("images", [])
            image_urls: list[str] = [
                img.get("url", "") for img in images if img.get("url")
            ]

            yield Layer2Staging(
                id=record["id"],
                image_urls=image_urls,
                image_url=image_urls[0] if image_urls else None,
                has_image=len(image_urls) > 0,
            )


@dlt.resource(
    name="raw_det_ingrs",
    write_disposition="replace",
    schema_contract={"columns": "evolve"},
)
def det_ingrs(data_dir: str) -> Iterator[DetIngrStaging]:
    """
    Extrait les ingrédients validés depuis det_ingrs.json via ijson en streaming.
    Logique équivalente au arrays_zip + F.filter de Spark, en Python pur.
    """
    path = Path(data_dir) / "det_ingrs.json"
    if not path.exists():
        return

    with open(path, "rb") as f:
        for record in ijson.items(f, "item"):
            ingredients: list[dict] = record.get("ingredients", [])
            valid_flags: list[bool] = record.get("valid", [])

            validated: list[str] = [
                ing.get("text", "").lower().strip()
                for ing, is_valid in zip(ingredients, valid_flags)
                if is_valid and ing.get("text")
            ]

            yield DetIngrStaging(
                id=record["id"],
                ingredients_validated=validated,
                n_ingredients_validated=len(validated),
            )


@dlt.resource(
    name="raw_nutrition",
    write_disposition="replace",
    schema_contract={"columns": "evolve"},
)
def nutrition(data_dir: str) -> Iterator[NutritionStaging]:
    """
    Extrait les valeurs nutritionnelles depuis le JSON MIT via ijson en streaming.
    Unités : kcal/100g pour l'énergie (standard Nutri-Score européen).
    """
    path = Path(data_dir) / "recipes_with_nutritional_info.json"
    if not path.exists():
        return

    with open(path, "rb") as f:
        for record in ijson.items(f, "item"):
            nutr = record.get("nutr_values_per100g", {})
            yield NutritionStaging(
                title=record.get("title", ""),
                energy=_safe_float(nutr.get("energy")),
                fat=_safe_float(nutr.get("fat")),
                protein=_safe_float(nutr.get("protein")),
                salt=_safe_float(nutr.get("salt")),
                saturates=_safe_float(nutr.get("saturates")),
                sugars=_safe_float(nutr.get("sugars")),
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
