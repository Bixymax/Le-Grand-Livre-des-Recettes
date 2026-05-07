# Données sources — `data/raw/`

Ce répertoire contient les 5 fichiers sources du pipeline. Ils ne sont pas
versionnés (voir `.gitignore`).

## Fichiers attendus

| Fichier | Source | Description |
|---------|--------|-------------|
| `layer1.json` | MIT Recipe1M+ | Recettes : titre, URL, instructions, ingrédients bruts |
| `layer2+.json` | MIT Recipe1M+ | Images associées aux recettes |
| `det_ingrs.json` | MIT Recipe1M+ | Ingrédients détectés avec flag de validation |
| `recipes_with_nutritional_info.json` | MIT Recipe1M+ | Macronutriments kcal/100g |
| `RAW_recipes.csv` | [Kaggle Food.com](https://www.kaggle.com/datasets/shuyangli94/food-com-recipes-and-user-interactions) | Recettes avec tags, temps de cuisson, description |

## Accès MIT Recipe1M+

Les fichiers MIT nécessitent une demande d'accès :
→ https://im2recipe.csail.mit.edu/im2recipe/

Une fois les fichiers obtenus, les déposer directement dans ce répertoire
en conservant les noms ci-dessus.

## Volumes attendus (indicatifs)

| Fichier | Taille | Enregistrements |
|---------|--------|-----------------|
| `layer1.json` | ~1.5 GB | ~1 000 000 |
| `layer2+.json` | ~300 MB | ~800 000 |
| `det_ingrs.json` | ~300 MB | ~1 000 000 |
| `recipes_with_nutritional_info.json` | ~50 MB | ~50 000 |
| `RAW_recipes.csv` | ~120 MB | ~230 000 |
