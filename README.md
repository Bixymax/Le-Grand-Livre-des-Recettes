# Le Grand Livre des Recettes — Pipeline ETL

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta--Spark-3.3.0-003366?logo=delta&logoColor=white)
![dlt](https://img.shields.io/badge/dlt-1.9%2B-8B5CF6)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Tests](https://img.shields.io/badge/Tests-pytest-yellow?logo=pytest&logoColor=white)

Pipeline ETL de données culinaires transformant **MIT Recipe1M+** et **Kaggle Food.com**
en tables **Delta Lake** pour un moteur de recherche de recettes.

- **Phase 1 — Ingestion** : `dlt` + `ijson` / `csv.DictReader` → lecture en streaming, normalisation Python, écriture Parquet (staging intermédiaire)
- **Phase 2 — Transformation** : `PySpark` → jointures LEFT JOIN, enrichissement, 3 tables finales Delta Lake (ACID, Z-Order, `delta_scan`)

---

## Architecture

```
data/raw/              (5 fichiers sources — non versionnés, voir data/raw/README.md)
  ├── layer1.json
  ├── layer2+.json
  ├── det_ingrs.json
  ├── recipes_with_nutritional_info.json
  └── RAW_recipes.csv

        │  Phase 1 — dlt  (ingestion / normalisation Python)
        ▼

data/staging/          (Parquets intermédiaires — auto-générés)
  ├── layer1/
  ├── layer2/
  ├── det_ingrs/
  ├── nutrition/
  └── kaggle/

        │  Phase 2 — PySpark  (jointures LEFT JOIN + enrichissement)
        ▼

data/outputs/delta/ (tables finales — auto-générées)
  ├── recipes_main/           partitionné par nutri_score
  ├── ingredients_index/
  └── recipes_nutrition_detail/
```

---

## Prérequis

- Docker + Docker Compose
- 5 fichiers sources dans `data/raw/` — voir [`data/raw/README.md`](data/raw/README.md)

---

## Démarrage

```bash
# 1. Démarrer le cluster Spark
docker compose up -d --build

# 2. Ouvrir un shell dans le container master
docker exec -it spark-master bash

# 3. (Dans le container) Lancer le pipeline complet
python run_pipeline.py run
```

Les commandes peuvent aussi être exécutées par phase :

```bash
python run_pipeline.py ingest             # Phase 1 dlt uniquement
python run_pipeline.py transform          # Phase 2 PySpark uniquement
python run_pipeline.py transform --master spark://spark-master:7077
python run_pipeline.py info               # Stats des tables finales
```

---

## Tests

La suite de tests couvre les sources dlt, les transformateurs Spark, le pipeline d'intégration et les utilitaires internes.

```bash
pytest tests/ -v
```

| Fichier | Contenu |
|---------|---------|
| `tests/test_sources.py` | Sources dlt (MIT & Kaggle) |
| `tests/test_transformers.py` | Transformateurs PySpark |
| `tests/test_pipeline.py` | Pipeline complet (unitaire) |
| `tests/test_integration.py` | Intégration bout-en-bout |
| `tests/conftest.py` | Fixtures partagées |

---

## UI

| Service | URL |
|---------|-----|
| Spark Master | http://localhost:8080 |
| Spark Application | http://localhost:4040 |
| Spark History Server | http://localhost:18080 |

---

## Tables de sortie

### `recipes_main`

Une ligne par recette. Partitionné physiquement par `nutri_score` pour accélérer
les requêtes filtrées côté moteur de recherche.

| Colonne | Type | Description |
|---------|------|-------------|
| `recipe_id` | string | ID MIT Recipe1M+ |
| `title` | string | Titre de la recette |
| `description` | string | Description Kaggle |
| `instructions_text` | string | Instructions concaténées (`" \| "`) |
| `ingredients_raw` | array[string] | Ingrédients bruts MIT |
| `ingredients_validated` | array[string] | Ingrédients validés MIT |
| `n_ingredients_validated` | int | Nombre d'ingrédients validés |
| `n_steps` | int | Nombre d'étapes (source MIT) |
| `cook_minutes` | int | Temps de cuisson Kaggle |
| `cook_time_category` | string | `rapide` / `moyen` / `long` / `inconnu` |
| `image_url` | string | URL de la première image |
| `image_urls` | array[string] | Toutes les URLs d'images |
| `has_image` | bool | Au moins une image disponible |
| `source_url` | string | URL de la page recette MIT |
| `mit_energy_kcal` | float | Énergie kcal/100g (MIT) → Nutri-Score |
| `kaggle_energy_kcal` | float | Énergie kcal/portion (Kaggle) → affichage |
| `nutri_score` | string | A / B / C / D / E (basé sur `mit_energy_kcal`) |
| `tags` | array[string] | Tags Kaggle |

### `ingredients_index`

Une ligne par *(recette × ingrédient)*. Permet des requêtes de filtrage par
ingrédient sans scanner les arrays de `recipes_main`.

| Colonne | Type |
|---------|------|
| `recipe_id` | string |
| `title` | string |
| `nutri_score` | string |
| `image_url` | string |
| `cook_time_category` | string |
| `ingredient` | string |

### `recipes_nutrition_detail`

Macronutriments g/100g par recette (source MIT uniquement). Ne contient que les
recettes avec au moins une valeur renseignée.

| Colonne | Type |
|---------|------|
| `recipe_id` | string |
| `fat_g` | float |
| `protein_g` | float |
| `salt_g` | float |
| `saturates_g` | float |
| `sugars_g` | float |

---

## Structure du projet

```
recipes-pipeline/
├── src/le_grand_livre_des_recettes/pipeline/
│   ├── config.py               Chemins, paramètres Spark, write_disposition dlt
│   ├── ingest.py               Orchestrateur Phase 1 (dlt)
│   ├── spark_session.py        Factory SparkSession (batch + streaming)
│   ├── models/
│   │   └── schemas.py          Contrats Pydantic (documentation)
│   ├── sources/
│   │   ├── _utils.py           normalize_title + log_progress (partagés)
│   │   ├── mit_recipes.py      dlt resources — MIT Recipe1M+
│   │   └── kaggle_recipes.py   dlt resource — Kaggle Food.com
│   └── transformers/
│       ├── assemble.py         Chargement Parquet + jointures
│       └── enrich.py           Colonnes dérivées + écriture tables finales
├── conf/spark-defaults.conf
├── sql/                        Référence DuckDB (ancienne implémentation)
├── data/raw/README.md          Instructions d'obtention des fichiers sources
├── .dlt/config.toml
├── docker-compose.yml
├── Dockerfile
├── entrypoint.sh
├── Makefile
├── pyproject.toml
├── requirements.txt
└── run_pipeline.py             CLI Typer (entrypoint Docker)
```

---

## Licence

MIT © 2026 Maxime Bourguignon