# 🍽️ Recipes Pipeline — dlt + DuckDB

Pipeline de data engineering pour le dataset MIT Recipe1M+ enrichi Food.com.
Architecture ELT : extraction + chargement via **dlt**, transformation via **SQL DuckDB**.

---

## Architecture

```
data/raw/                          data/outputs/
  layer1.json         ─────┐         recipes_catalog.duckdb
  layer2+.json         ─────┤──dlt──▶   recipes.raw_layer1
  det_ingrs.json       ─────┤           recipes.raw_layer2
  nutrition.json       ─────┘           recipes.raw_det_ingrs
  RAW_recipes.csv ─────────────────▶    recipes.raw_kaggle
                                         recipes.raw_nutrition
                              SQL ──▶   recipes.v_assembled       (vue jointure)
                                         recipes.recipes_main      (table finale)
                                         recipes.ingredients_index (table finale)
                                         recipes.recipes_nutrition_detail (table finale)
```

### Pourquoi cette séparation ELT ?

| Étape | Outil | Rôle |
|-------|-------|------|
| **Extract** | dlt `@resource` (Python pur) | Lire les JSON/CSV, normaliser inline |
| **Load** | dlt `pipeline.run()` | Écrire vers DuckDB (dev) ou Delta (prod) |
| **Transform** | SQL DuckDB | Jointures, enrichissements, index FTS |

dlt gère le state, le schema, le retry et les logs.
DuckDB gère les jointures sur 1M+ lignes en quelques secondes.

---

## Structure

```
recipes-pipeline/
├── .dlt/
│   ├── config.toml           # Config pipeline (destination, chemins)
│   └── secrets.toml          # Credentials (gitignored)
├── pipeline/
│   ├── sources/
│   │   ├── mit_recipes.py    # @dlt.source : layer1, layer2, det_ingrs, nutrition
│   │   └── kaggle_recipes.py # @dlt.source : RAW_recipes.csv
│   ├── transformers/
│   │   └── enrich.py         # Logique pure : nutri_score, cook_time_cat, coalesce
│   └── models/
│       └── recipes.py        # Pydantic schemas (contrats de données)
├── sql/
│   ├── 01_assemble.sql       # Vue v_assembled (jointures en cascade)
│   └── 02_final_tables.sql   # 3 tables finales + index + FTS
├── tests/
│   ├── test_sources.py       # Tests normalisation (normalize_title, parse_list…)
│   └── test_transformers.py  # Tests logique métier (nutri_score, cook_time…)
├── run_pipeline.py           # CLI Typer : run / ingest / transform / info
└── pyproject.toml
```

---

## Installation

```bash
python -m venv .venv
source .venv/bin/activate          # Windows : .venv\Scripts\activate
pip install -e ".[dev]"
```

Placez vos fichiers sources dans `data/raw/` :
```
data/raw/
  layer1.json
  layer2+.json
  det_ingrs.json
  recipes_with_nutritional_info.json
  RAW_recipes.csv
```

---

## Utilisation

```bash
# Pipeline complet (recommandé)
python run_pipeline.py run

# Étapes séparées
python run_pipeline.py ingest      # dlt uniquement → staging
python run_pipeline.py transform   # SQL uniquement → tables finales

# Destination Delta Lake (production / futur streaming)
python run_pipeline.py run --dest delta

# Stats des tables finales
python run_pipeline.py info
```

---

## Tests

```bash
pytest tests/ -v
pytest tests/ --cov=src/le_grand_livre_des_recettes/pipeline --cov-report=term-missing
```

Les tests unitaires (`test_transformers.py`, `test_sources.py`) ne nécessitent
aucun fichier de données — ils testent la logique pure Python.

---

## Vers le streaming (prochaine étape)

La séparation `@dlt.resource` / SQL est pensée pour le streaming :

1. **Passer `write_disposition="append"`** dans les resources → dlt gère l'état incrémental
2. **Activer le Delta Change Data Feed** sur les tables finales → les consumers streaming
   peuvent lire uniquement les deltas
3. **Remplacer la destination `duckdb` par `filesystem` (Delta)** pour un stockage
   compatible Spark Structured Streaming / Kafka Connect
