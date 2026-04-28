"""
Fixtures partagées entre tous les modules de test.

`pipeline_db` — scope "session" : lance le pipeline complet (dlt + SQL)
une seule fois sur les mini-fixtures et retourne le chemin du DuckDB résultant.
Tous les tests d'intégration le consomment en lecture seule.
"""

from __future__ import annotations

import logging
from pathlib import Path

import dlt
import duckdb
import pytest

from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import mit_recipes_source
from src.le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import kaggle_recipes_source

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SQL_DIR = Path(__file__).parent.parent / "sql"

log = logging.getLogger(__name__)

# Préfixes de statements qu'on ignore silencieusement dans l'environnement de test :
#  - INSTALL / LOAD   : extensions réseau (FTS, httpfs…) non disponibles en CI
#  - SELECT seul      : sanity-checks en fin de fichier SQL, pas critiques
#  - PRAGMA           : FTS index, dépend de l'extension FTS
_IGNORABLE_PREFIXES = ("install ", "load ", "select ", "pragma ")


def _is_ignorable(stmt: str) -> bool:
    first = stmt.strip().lower().split()[0] if stmt.strip() else ""
    return first in ("install", "load", "select", "pragma")


@pytest.fixture(scope="session")
def pipeline_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Lance le pipeline complet sur les mini-fixtures (3 recettes MIT + 1 Kaggle).
    Retourne le chemin du DuckDB produit — consommé en lecture seule par les tests.

    Scope "session" : le setup coûteux ne tourne qu'une seule fois même si
    plusieurs classes de test utilisent la fixture.
    """
    tmp = tmp_path_factory.mktemp("integration_db")
    db_path = tmp / "test_recipes.duckdb"

    # -------------------------------------------------------------------------
    # Étape 1 : ingestion dlt
    #   - Pas de loader_file_format="parquet" : parquet requiert pyarrow.
    #   - Le format jsonl (défaut dlt) est suffisant pour les tests.
    # -------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name="test_recipes_pipeline",
        destination=dlt.destinations.duckdb(credentials=str(db_path)),
        dataset_name="recipes",
    )
    load_info = pipeline.run(
        [
            mit_recipes_source(data_dir=str(FIXTURES_DIR)),
            kaggle_recipes_source(data_dir=str(FIXTURES_DIR)),
        ]
    )
    assert not load_info.has_failed_jobs, \
        f"dlt ingestion failed: {load_info}"

    # -------------------------------------------------------------------------
    # Étape 2 : transformations SQL
    #   Les statements ignorables (INSTALL/LOAD/SELECT/PRAGMA) sont skippés
    #   silencieusement — FTS n'est pas disponible en environnement de test.
    #   Toutes les autres erreurs propagent un AssertionError immédiat.
    # -------------------------------------------------------------------------
    with duckdb.connect(str(db_path)) as con:
        con.execute(f"SET temp_directory='{tmp}'")

        for sql_file in sorted(SQL_DIR.glob("*.sql")):
            sql_text = sql_file.read_text(encoding="utf-8")

            # Découpage en statements : on split sur ';' et on ignore uniquement
            # les segments vides. Les blocs de commentaires purs sont traités
            # dans la boucle ci-dessous après stripping des lignes de commentaires.
            statements = [
                s.strip()
                for s in sql_text.split(";")
                if s.strip()
            ]

            for stmt in statements:
                # Retirer les lignes de commentaires en tête pour identifier
                # correctement le type du statement (CREATE, SELECT, INSTALL…)
                # sans tomber sur un bloc -- commentaire\nCREATE TABLE qui serait
                # classifié comme commentaire pur et ignoré à tort.
                code_lines = [
                    l for l in stmt.split("\n")
                    if l.strip() and not l.strip().startswith("--")
                ]
                code_body = "\n".join(code_lines).strip()

                if not code_body:
                    # Bloc 100% commentaires — ignorer silencieusement
                    continue

                if _is_ignorable(code_body):
                    log.debug("SKIP (test env): %.80s", code_body)
                    continue

                try:
                    con.execute(stmt)
                except Exception as exc:
                    raise AssertionError(
                        f"\n\nSQL failed in {sql_file.name}:\n"
                        f"  {stmt[:300]!r}\n"
                        f"  Error: {exc}"
                    ) from exc

    return db_path
