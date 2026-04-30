# Pipeline Recipes — PySpark

Remplacement du pipeline `dlt + DuckDB` par un pipeline **PySpark pur**.

## Architecture

```
Phase 1 — Ingestion (ingest)
  data/raw/*.json + *.csv
      ↓  Spark readers avec schémas explicites
  data/staging/*.parquet

Phase 2 — Transformation (transform)
  data/staging/*.parquet
      ↓  Jointures LEFT JOIN + enrichissement
  data/outputs/parquets/
      ├── recipes_main/            (partitionné par nutri_score)
      ├── ingredients_index/
      └── recipes_nutrition_detail/
```

## Lancement

### Cluster Docker (recommandé)
```bash
# 1. Démarrer le cluster
docker-compose up

# 2. Depuis le master node OU depuis l'hôte avec SPARK_MASTER_URL :
export SPARK_MASTER_URL=spark://spark-master:7077

# Pipeline complet
python run_pipeline.py run

# Ou avec l'option --master
python run_pipeline.py run --master spark://spark-master:7077
```

### Mode local (dev / tests)
```bash
# Aucune variable d'env nécessaire — utilise local[*] par défaut
python run_pipeline.py run
```

### Commandes disponibles
```bash
python run_pipeline.py run        # Phase 1 + Phase 2
python run_pipeline.py ingest     # Phase 1 uniquement (staging)
python run_pipeline.py transform  # Phase 2 uniquement (tables finales)
python run_pipeline.py info       # stats sur les tables finales
```

## Structure du projet

```
run_pipeline.py                            ← entrypoint CLI
src/le_grand_livre_des_recettes/pipeline/
  spark_session.py                         ← factory SparkSession
  config.py                                ← chemins centralisés
  sources/
    mit_recipes.py                         ← lecture layer1/2, det_ingrs, nutrition
    kaggle_recipes.py                      ← lecture RAW_recipes.csv
  transformers/
    assemble.py                            ← jointures (Phase 2a)
    enrich.py                              ← nutri_score + 3 tables finales (Phase 2b)
  models/
    recipes.py                             ← schémas Pydantic (documentation)
```

## Tables finales

| Table | Granularité | Partitionnement |
|---|---|---|
| `recipes_main` | 1 ligne / recette | `nutri_score` (A-E) |
| `ingredients_index` | 1 ligne / (recette, ingrédient) | aucun |
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
