# 🍽️ Recipes Pipeline — dlt + DuckDB

Pipeline de data engineering pour le dataset MIT Recipe1M+ enrichi Food.com.
Architecture ELT : extraction + chargement via **dlt**, transformation via **SQL DuckDB**.

---

## Architecture

```
data/raw/                                      data/outputs/
  layer1.json                  ─────┐            recipes_catalog.duckdb
  layer2+.json                  ─────┤──dlt──▶     recipes.raw_layer1
  det_ingrs.json                ─────┤             recipes.raw_layer2
  recipes_with_nutritional_info.json ─┘            recipes.raw_det_ingrs
  RAW_recipes.csv ──────────────────────────▶     recipes.raw_kaggle
                                                    recipes.raw_nutrition

                                       SQL ──▶   recipes.v_assembled            (vue jointure)
                                                    recipes.recipes_main         (table finale)
                                                    recipes.ingredients_index    (table finale)
                                                    recipes.recipes_nutrition_detail (table finale)
```

> **Colonnes énergie dans `recipes_main` :**
> | Colonne | Unité | Source | Usage |
> |---|---|---|---|
> | `mit_energy_kcal` | kcal / 100 g | MIT Recipe1M+ | Nutri-Score (standard européen) |
> | `kaggle_energy_kcal` | kcal / portion | Food.com | Affichage uniquement |
>
> Ces deux colonnes ne sont **jamais fusionnées** — leurs unités sont incompatibles.
> Le Nutri-Score est calculé **exclusivement** sur `mit_energy_kcal`.

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
le_grand_livre_des_recettes/          ← racine du repo
├── .dlt/
│   ├── config.toml                   # Config pipeline (destination, chemins data_dir)
│   └── secrets.toml                  # Credentials Kaggle / Delta (gitignored)
├── src/
│   └── le_grand_livre_des_recettes/
│       └── pipeline/
│           ├── sources/
│           │   ├── mit_recipes.py    # @dlt.source : layer1, layer2, det_ingrs, nutrition
│           │   └── kaggle_recipes.py # @dlt.source : RAW_recipes.csv
│           ├── transformers/
│           │   └── enrich.py         # Logique pure : nutri_score, cook_time_cat, coalesce_energy
│           └── models/
│               └── recipes.py        # Pydantic schemas — Raw*, *Staging (câblés dlt), Recipe*
├── sql/
│   ├── 01_assemble.sql               # Vue v_assembled (jointures en cascade MIT + Kaggle)
│   └── 02_final_tables.sql           # 3 tables finales + index B-tree + FTS (stemmer english)
├── tests/
│   ├── fixtures/                     # Mini-dataset (3 recettes) pour les tests d'intégration
│   │   ├── layer1.json
│   │   ├── layer2+.json
│   │   ├── det_ingrs.json
│   │   ├── recipes_with_nutritional_info.json
│   │   └── RAW_recipes.csv
│   ├── conftest.py                   # Fixture session `pipeline_db` — pipeline complet sur fixtures
│   ├── test_sources.py               # Tests unitaires : normalize_title, parse_list, parse_nutrition…
│   ├── test_transformers.py          # Tests unitaires : nutri_score, cook_time_cat, coalesce_energy
│   └── test_integration.py           # Tests d'intégration : staging, tables finales, sémantique énergie
├── data/
│   ├── raw/                          # Fichiers sources bruts (non versionné — trop volumineux)
│   └── outputs/                      # recipes_catalog.duckdb (généré — gitignored)
├── run_pipeline.py                   # CLI Typer : run / ingest / transform / info
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
# Tous les tests (unitaires + intégration)
pytest tests/ -v

# Unitaires uniquement — aucune dépendance externe, très rapides
pytest tests/test_sources.py tests/test_transformers.py -v

# Intégration uniquement — lance le pipeline complet sur les mini-fixtures
pytest tests/test_integration.py -v

# Couverture de code
pytest tests/ --cov=src/le_grand_livre_des_recettes/pipeline --cov-report=term-missing
```

### Deux niveaux de tests

| Type | Fichier | Données requises | Vitesse |
|------|---------|-----------------|---------|
| **Unitaires** | `test_sources.py`, `test_transformers.py` | Aucune | < 1 s |
| **Intégration** | `test_integration.py` | `tests/fixtures/` (3 recettes) | ~10 s |

Les tests d'intégration couvrent :
- Peuplement correct des tables staging (`raw_layer1`, `raw_layer2`, etc.)
- Tables finales (`recipes_main`, `ingredients_index`, `recipes_nutrition_detail`)
- **Sémantique énergie** : `nutri_score` calculé uniquement sur `mit_energy_kcal` (kcal/100g), jamais sur `kaggle_energy_kcal` (kcal/portion)
- Exclusion des ingrédients `valid=False` de l'index
- Pattern de filtre AND/OR sur `ingredients_index`

La fixture `pipeline_db` (scope `session`) tourne le pipeline complet une seule fois,
peu importe le nombre de classes de test qui la consomment.

---

## Vers le streaming (prochaine étape)

La séparation `@dlt.resource` / SQL est pensée pour le streaming :

1. **Passer `write_disposition="append"`** dans les resources → dlt gère l'état incrémental
2. **Activer le Delta Change Data Feed** sur les tables finales → les consumers streaming
   peuvent lire uniquement les deltas
3. **Remplacer la destination `duckdb` par `filesystem` (Delta)** pour un stockage
   compatible Spark Structured Streaming / Kafka Connect
