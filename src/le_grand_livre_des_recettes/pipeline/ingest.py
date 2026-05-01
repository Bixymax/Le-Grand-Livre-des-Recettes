"""
Phase 1 — Ingestion via dlt.

dlt charge les fichiers sources (JSON + CSV), les normalise en Python
et les écrit en Parquet dans ``data/staging/`` via la destination filesystem.

Tables staging créées :
    layer1/      ← MIT layer1.json
    layer2/      ← MIT layer2+.json
    det_ingrs/   ← MIT det_ingrs.json
    nutrition/   ← MIT recipes_with_nutritional_info.json
    kaggle/      ← Kaggle RAW_recipes.csv

Ces répertoires sont ensuite lus par PySpark en Phase 2.

Chaque table est chargée séparément : si une table échoue, les tables déjà
écrites dans le run courant sont conservées et le message d'erreur indique
précisément quelle table est en cause.

Chargement incrémental (production) :
    Passer ``DLT_WRITE_DISPOSITION = "append"`` dans ``config.py``.
    dlt ajoutera les nouveaux enregistrements sans écraser les existants,
    ce qui permet d'alimenter la base avec de nouvelles recettes au fil du temps.
"""

from __future__ import annotations

import dlt

from le_grand_livre_des_recettes.pipeline import config as cfg
from le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import kaggle_resource
from le_grand_livre_des_recettes.pipeline.sources.mit_recipes import det_ingrs, layer1, layer2, nutrition

# Ordre de chargement : layer1 en premier car c'est la table de base des jointures
_RESOURCES = [
    ("layer1",    layer1),
    ("layer2",    layer2),
    ("det_ingrs", det_ingrs),
    ("nutrition", nutrition),
    ("kaggle",    kaggle_resource),
]


def run_ingestion() -> None:
    """
    Exécute le pipeline dlt d'ingestion, une table à la fois.

    Chaque ressource est commitée indépendamment : une erreur sur une table
    n'annule pas les tables déjà écrites lors du même run.

    ``write_disposition`` est contrôlé par ``config.DLT_WRITE_DISPOSITION`` :
        - ``"replace"``  → recharge complète à chaque run (défaut)
        - ``"append"``   → ajout incrémental pour alimenter la production

    Raises
    ------
    Exception
        Propage l'erreur dlt de la table en échec sans masquer le contexte.
    """
    pipeline = dlt.pipeline(
        pipeline_name="recipes_ingest",
        destination="filesystem",
        dataset_name="staging",
    )

    for name, resource_fn in _RESOURCES:
        print(f"\n── Table : {name} ──────────────────────────────────────")
        load_info = pipeline.run(
            resource_fn(),
            loader_file_format="parquet",
        )
        print(load_info)