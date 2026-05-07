"""
Phase 1 - Ingestion via dlt.

Charge les fichiers sources (JSON et CSV), les normalise et les écrit
au format Delta Lake dans le répertoire de staging.

Tables générées :
- layer1 : MIT layer1.json
- layer2 : MIT layer2+.json
- det_ingrs : MIT det_ingrs.json
- nutrition : MIT recipes_with_nutritional_info.json
- kaggle : Kaggle RAW_recipes.csv
"""

from __future__ import annotations

from typing import Any, Callable

import dlt

from le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import kaggle_resource
from le_grand_livre_des_recettes.pipeline.sources.mit_recipes import (
    det_ingrs,
    layer1,
    layer2,
    nutrition,
)

_RESOURCES: list[tuple[str, Callable[..., Any]]] = [
    ("layer1", layer1),
    ("layer2", layer2),
    ("det_ingrs", det_ingrs),
    ("nutrition", nutrition),
    ("kaggle", kaggle_resource),
]


def run_ingestion() -> None:
    """
    Exécute le pipeline dlt d'ingestion.

    Chaque ressource est traitée de manière indépendante. La stratégie d'écriture
    dépend de la configuration dlt.

    Raises:
        Exception: Propage l'erreur dlt de la table en échec.
    """
    pipeline = dlt.pipeline(
        pipeline_name="recipes_ingest",
        destination="filesystem",
        dataset_name="staging",
    )

    for name, resource_fn in _RESOURCES:
        print(f"\n--- Table : {name} ---")
        load_info = pipeline.run(
            resource_fn(),
            loader_file_format="delta",
        )
        print(load_info)
