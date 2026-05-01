"""
Source dlt — Fichiers MIT Recipe1M+.

Stream les fichiers JSON via ijson pour éviter la saturation mémoire.
Les ressources extraient respectivement les recettes de base, les images,
les ingrédients validés et les macronutriments (kcal/100g).
"""

from __future__ import annotations

import time
from typing import Iterator, Any

import dlt
import ijson

from le_grand_livre_des_recettes.pipeline import config as cfg
from le_grand_livre_des_recettes.pipeline.sources._utils import log_progress, normalize_title


@dlt.resource(
    name="layer1",
    write_disposition=cfg.DLT_WRITE_DISPOSITION,
    columns={"ingredients_raw": {"data_type": "complex"}},
)
def layer1() -> Iterator[dict[str, Any]]:
    """Yield les recettes brutes depuis layer1.json."""
    print(f"  [layer1] Lecture de {cfg.LAYER1_PATH} ...", flush=True)
    t0 = time.perf_counter()
    count = 0

    with open(cfg.LAYER1_PATH, "rb") as f:
        for r in ijson.items(f, "item"):
            instructions = r.get("instructions") or []
            ingredients = r.get("ingredients") or []
            yield {
                "id": str(r["id"]),
                "title": r.get("title"),
                "url": r.get("url"),
                "partition": r.get("partition"),
                "instructions_text": " | ".join(s.get("text", "") for s in instructions),
                "ingredients_raw": [i.get("text", "") for i in ingredients],
                "n_steps": len(instructions),
                "title_norm": normalize_title(r.get("title", "")),
            }
            count += 1
            log_progress("layer1", count, t0)

    print(f"  [layer1] TERMINE : {count:,} recettes en {time.perf_counter() - t0:.1f}s", flush=True)


@dlt.resource(
    name="layer2",
    write_disposition=cfg.DLT_WRITE_DISPOSITION,
    columns={"image_urls": {"data_type": "complex"}},
)
def layer2() -> Iterator[dict[str, Any]]:
    """Yield les associations d'images depuis layer2+.json."""
    print(f"  [layer2] Lecture de {cfg.LAYER2_PATH} ...", flush=True)
    t0 = time.perf_counter()
    count = 0

    with open(cfg.LAYER2_PATH, "rb") as f:
        for r in ijson.items(f, "item"):
            images = r.get("images") or []
            urls = [img.get("url", "") for img in images]
            yield {
                "id": str(r["id"]),
                "image_url": urls[0] if urls else None,
                "image_urls": urls,
                "has_image": len(urls) > 0,
            }
            count += 1
            log_progress("layer2", count, t0)

    print(f"  [layer2] TERMINE : {count:,} entrees en {time.perf_counter() - t0:.1f}s", flush=True)


@dlt.resource(
    name="det_ingrs",
    write_disposition=cfg.DLT_WRITE_DISPOSITION,
    columns={"ingredients_validated": {"data_type": "complex"}},
)
def det_ingrs() -> Iterator[dict[str, Any]]:
    """Yield les ingrédients validés (filtrage via tableau booléen parallèle)."""
    print(f"  [det_ingrs] Lecture de {cfg.DET_INGRS_PATH} ...", flush=True)
    t0 = time.perf_counter()
    count = 0

    with open(cfg.DET_INGRS_PATH, "rb") as f:
        for r in ijson.items(f, "item"):
            raw_ingrs = r.get("ingredients") or []
            valid = r.get("valid") or []
            validated = [
                ingr.get("text", "").lower().strip()
                for ingr, v in zip(raw_ingrs, valid)
                if v and ingr.get("text")
            ]
            yield {
                "id": str(r["id"]),
                "ingredients_validated": validated,
                "n_ingredients_validated": len(validated),
            }
            count += 1
            log_progress("det_ingrs", count, t0)

    print(f"  [det_ingrs] TERMINE : {count:,} entrees en {time.perf_counter() - t0:.1f}s", flush=True)


@dlt.resource(
    name="nutrition",
    write_disposition=cfg.DLT_WRITE_DISPOSITION,
    columns={
        "energy_kcal": {"data_type": "double"},
        "fat_g": {"data_type": "double"},
        "protein_g": {"data_type": "double"},
        "salt_g": {"data_type": "double"},
        "saturates_g": {"data_type": "double"},
        "sugars_g": {"data_type": "double"},
    },
)
def nutrition() -> Iterator[dict[str, Any]]:
    """Yield les données macro-nutritionnelles pour 100g."""
    print(f"  [nutrition] Lecture de {cfg.NUTR_PATH} ...", flush=True)
    t0 = time.perf_counter()
    count = 0

    with open(cfg.NUTR_PATH, "rb") as f:
        for r in ijson.items(f, "item"):
            nutr = r.get("nutr_values_per100g") or {}
            yield {
                "title": r.get("title"),
                "title_norm": normalize_title(r.get("title", "")),
                "energy_kcal": nutr.get("energy"),
                "fat_g": nutr.get("fat"),
                "protein_g": nutr.get("protein"),
                "salt_g": nutr.get("salt"),
                "saturates_g": nutr.get("saturates"),
                "sugars_g": nutr.get("sugars"),
            }
            count += 1
            log_progress("nutrition", count, t0)

    print(f"  [nutrition] TERMINE : {count:,} entrees en {time.perf_counter() - t0:.1f}s", flush=True)


@dlt.source(name="mit_recipes")
def mit_source():
    """Point d'entrée de la source dlt pour le dataset MIT Recipe1M+."""
    return layer1(), layer2(), det_ingrs(), nutrition()