# Pipeline Recipes — PySpark

## Architecture

```
Phase 1 — Ingestion (dlt)
  data/raw/*.json + *.csv
      ↓  dlt resources (normalisation Python, streaming ijson / csv.DictReader)
  data/staging/  (Parquet — intermédiaire jetable, géré par dlt)
      layer1/  layer2/  det_ingrs/  nutrition/  kaggle/

Phase 2 — Transformation (PySpark → Delta Lake)
  data/staging/  (Parquet)
      ↓  Jointures LEFT JOIN + enrichissement + Z-Order
  data/outputs/delta/  (Delta Lake — ACID, time travel, delta_scan DuckDB)
      ├── recipes_main/            (partitionné par nutri_score)
      ├── ingredients_index/
      └── recipes_nutrition_detail/
```

## Lancement

```bash
# 1. Démarrer le cluster
docker compose up -d --build

# 2. Entrer dans le container master
docker exec -it spark-master bash

# 3. Lancer le pipeline
python run_pipeline.py run
```

### Commandes disponibles

```bash
python run_pipeline.py run                                    # Phase 1 + Phase 2
python run_pipeline.py ingest                                 # Phase 1 uniquement (staging)
python run_pipeline.py transform                              # Phase 2 uniquement (tables finales)
python run_pipeline.py transform --master spark://spark-master:7077
python run_pipeline.py info                                   # stats sur les tables finales
```

## Structure du projet

```
run_pipeline.py                            ← entrypoint CLI
src/le_grand_livre_des_recettes/pipeline/
  config.py                                ← chemins et paramètres centralisés
  ingest.py                                ← orchestrateur dlt (Phase 1)
  spark_session.py                         ← factory SparkSession
  sources/
    _utils.py                              ← normalize_title, log_progress (partagés)
    mit_recipes.py                         ← dlt resources — layer1/2, det_ingrs, nutrition
    kaggle_recipes.py                      ← dlt resource — RAW_recipes.csv
  transformers/
    assemble.py                            ← chargement Parquet + jointures (Phase 2a)
    enrich.py                              ← colonnes dérivées + 3 tables finales (Phase 2b)
  models/
    schemas.py                             ← schémas Pydantic (documentation)
```

## Tables finales

| Table | Granularité | Partitionnement |
|---|---|---|
| `recipes_main` | 1 ligne / recette | `nutri_score` (A–E) |
| `ingredients_index` | 1 ligne / (recette × ingrédient) | aucun |
| `recipes_nutrition_detail` | 1 ligne / recette avec données MIT | aucun |

## Nutri-Score

Calculé **uniquement** sur `mit_energy_kcal` (kcal/100g, source MIT).  
`kaggle_energy_kcal` est en kcal/portion — ne pas utiliser pour le Nutri-Score.

| Score | kcal/100g |
|---|---|
| A | < 80 |
| B | 80–159 |
| C | 160–269 |
| D | 270–399 |
| E | ≥ 400 |

## Chargement incrémental en production

Changer dans `config.py` :
```python
DLT_WRITE_DISPOSITION: str = "append"  # au lieu de "replace"
```
dlt ajoutera les nouvelles recettes sans écraser les données existantes.