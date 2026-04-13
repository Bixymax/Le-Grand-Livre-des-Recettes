# Documentation Technique — Pipeline de Transformation de Datasets de Recettes

> **Contexte :** Ce pipeline Databricks (PySpark) transforme plusieurs datasets hétérogènes de recettes (MIT Recipe1M+ et Food.com / Kaggle) en un ensemble de tables Delta Lake optimisées, prêtes à alimenter un moteur de recherche et un grand livre de recettes interactif.

---

## Table des Matières

1. [Vue d'ensemble de l'architecture](#1-vue-densemble-de-larchitecture)
2. [Sources de données](#2-sources-de-données)
3. [Phase 1 — Ingestion & Staging (JSON → Parquet)](#3-phase-1--ingestion--staging-json--parquet)
4. [Phase 2 — Assemblage & Enrichissement](#4-phase-2--assemblage--enrichissement)
5. [Décisions d'optimisation Spark](#5-décisions-doptimisation-spark)
6. [Modèle de données final](#6-modèle-de-données-final)
7. [Optimisations Databricks (Delta & Z-Order)](#7-optimisations-databricks-delta--z-order)
8. [Conception orientée usage final (UI/API)](#8-conception-orientée-usage-final-uiapi)

---

## 1. Vue d'ensemble de l'architecture

Le pipeline est structuré en **deux phases séquentielles** séparées par un staging intermédiaire sur disque :

```
[Sources JSON/CSV brutes]
         │
         ▼
  ┌─────────────┐
  │   PHASE 1   │  ← Parsing, nettoyage, normalisation
  │  Staging    │     Écriture en Parquet (format colonnaire)
  └─────────────┘
         │
         ▼
  ┌─────────────┐
  │   PHASE 2   │  ← Jointures, enrichissement, calculs dérivés
  │  Assemblage │     Écriture en Delta Lake partitionné + Z-Order
  └─────────────┘
         │
         ▼
[3 tables Delta finales]
  recipes_main  │  ingredients_index  │  recipes_nutrition_detail
```

Cette séparation en deux phases n'est pas arbitraire : elle évite les **full re-scans** JSON coûteux à chaque itération du pipeline, et permet de travailler sur des fichiers Parquet compressés et à schéma fixe lors de la phase d'assemblage.

---

## 2. Sources de données

| Dataset | Format | Clé de jointure | Contenu principal |
|---|---|---|---|
| `layer1.json` | JSON multi-lignes | `id` | Titre, URL, instructions, ingrédients bruts |
| `layer2+.json` | JSON multi-lignes | `id` | URLs des images associées |
| `det_ingrs.json` | JSON multi-lignes | `id` | Ingrédients avec flag de validité booléen |
| `recipes_with_nutritional_info.json` | JSON multi-lignes | `title_norm` | Valeurs nutritionnelles per 100g |
| `RAW_recipes.csv` | CSV | `title_norm` | Temps de cuisson, tags, description (Kaggle/Food.com) |

> **Remarque sur les clés de jointure :** Les datasets MIT (layer1, layer2, det_ingrs) partagent un `id` commun. En revanche, les datasets nutritionnels et Kaggle ne possèdent pas cet identifiant — la jointure se fait donc sur `title_norm`, une clé dérivée normalisée (voir section 3.4).

---

## 3. Phase 1 — Ingestion & Staging (JSON → Parquet)

### 3.1 Définition explicite des schémas

Tous les fichiers JSON sont lus avec un **schéma explicite** (`StructType`), plutôt qu'avec `inferSchema=True`.

**Pourquoi ?** Sur des datasets de plusieurs millions de lignes, l'inférence de schéma oblige Spark à effectuer une **lecture complète du fichier une première fois** pour déduire les types. Avec un schéma défini manuellement :
- La lecture est réalisée en un seul passage.
- Les colonnes superflues ne sont jamais chargées en mémoire.
- Le comportement est déterministe, même si le dataset contient des lignes malformées.

### 3.2 Extraction des tableaux imbriqués avec `F.transform` (sans UDF)

Les datasets JSON contiennent des structures imbriquées (ex: `ingredients: [{"text": "..."}, ...]`). Pour en extraire le texte, le pipeline utilise systématiquement `F.transform` (fonctions natives Spark) plutôt que des UDFs Python.

```python
# Extraction du texte des ingrédients imbriqués
.withColumn("ingredients_raw", F.transform("ingredients", lambda x: x["text"]))
```

**Pourquoi éviter les UDFs ?**

Les UDFs Python forcent Spark à **sérialiser chaque ligne depuis la JVM vers l'interpréteur Python**, puis à la désérialiser dans l'autre sens. Ce franchissement de frontière JVM/Python représente le principal goulot d'étranglement en termes de performance sur les traitements de masse. `F.transform` s'exécute entièrement dans la JVM, sans cette pénalité.

### 3.3 Filtrage des ingrédients validés avec `arrays_zip` + `F.filter`

Le dataset `det_ingrs.json` contient deux tableaux parallèles : une liste de textes d'ingrédients et une liste de booléens (`valid`). Pour ne conserver que les ingrédients valides, on ne peut pas filtrer directement un tableau basé sur un autre.

La solution adoptée utilise `F.arrays_zip` pour fusionner les deux tableaux en un seul tableau de structs, puis `F.filter` pour éliminer les entrées invalides :

```python
.withColumn("zipped", F.arrays_zip("ingr_texts", "valid"))
.withColumn("ingredients_validated",
    F.transform(
        F.filter("zipped", lambda x: x["valid"] == True),
        lambda x: x["ingr_texts"]
    ))
```

Cette approche reste 100% dans la JVM et évite tout `explode` intermédiaire qui aurait multiplié le nombre de lignes.

### 3.4 Normalisation agressive pour la clé de jointure (`title_norm`)

Les titres de recettes varient entre datasets : majuscules différentes, apostrophes, tirets, accents. Faire une jointure directe sur `title` produirait des taux de correspondance catastrophiques.

La clé `title_norm` est construite ainsi :

```python
F.lower(F.trim(F.regexp_replace("title", r"[^a-zA-Z0-9\s]", "")))
```

- **`lower`** : insensibilité à la casse
- **`trim`** : suppression des espaces avant/après
- **`regexp_replace`** : suppression de toute ponctuation, caractères spéciaux, apostrophes

**Exemple :**  
`"Chicken Tikka Masala!"` → `"chicken tikka masala"`  
`"Chicken Tikka Masala"` (Kaggle) → `"chicken tikka masala"` ✓ Jointure réussie.

### 3.5 Nettoyage du CSV Kaggle

Le CSV Kaggle utilise des colonnes `tags` et `nutrition` stockées comme des chaînes de caractères représentant des listes Python (ex: `"[tag1, tag2, tag3]"`). Ce format requiert un nettoyage spécifique :

```python
# Tags : suppression des crochets et guillemets, puis split
.withColumn("tags", F.split(F.regexp_replace(F.regexp_replace("tags_raw", r"[\[\]']", ""), r",\s+", ","), ","))

# Nutrition : extraction de la valeur calorique (premier élément)
.withColumn("nutrition_array", F.split(F.regexp_replace("nutrition_raw", r"[\[\]]", ""), ","))
.withColumn("kaggle_energy_kcal", F.col("nutrition_array")[0].cast(FloatType()))
```

Un `dropDuplicates(["title_norm"])` est appliqué à ce stade pour éviter de polluer les jointures ultérieures avec des doublons.

---

## 4. Phase 2 — Assemblage & Enrichissement

### 4.1 Stratégie de jointures en cascade (LEFT JOIN)

L'assemblage est centré sur `layer1` comme table de référence. Toutes les jointures sont des **LEFT JOINs** :

```
layer1
  LEFT JOIN layer2       ON id          (images)
  LEFT JOIN det_ingrs    ON id          (ingrédients validés)
  LEFT JOIN nutrition    ON title_norm  (données nutritionnelles)
  LEFT JOIN kaggle       ON title_norm  (métadonnées Food.com)
```

**Pourquoi LEFT JOIN systématiquement ?**  
Un INNER JOIN éliminerait silencieusement toutes les recettes sans image ou sans données nutritionnelles. Or, une recette sans image reste une recette valide et utile. Le LEFT JOIN garantit que **aucune recette de layer1 n'est perdue**, quelles que soient les lacunes des autres sources.

### 4.2 Réconciliation des données nutritionnelles (`F.coalesce`)

Deux sources fournissent des données caloriques : le dataset nutritionnel MIT et le CSV Kaggle. Plutôt que de choisir arbitrairement l'une ou l'autre, `F.coalesce` applique une logique de fallback :

```python
.withColumn("energy_kcal", F.coalesce(F.col("energy_kcal"), F.col("kaggle_energy_kcal")))
```

La première valeur non-null est retenue. Le dataset MIT est prioritaire (données plus précises, per 100g), Kaggle sert de fallback pour maximiser le taux de couverture nutritionnelle.

### 4.3 Calcul du Nutri-Score simplifié

Le Nutri-Score est calculé **dans la Phase 2** (et non dans la Phase 1), car il nécessite la valeur `energy_kcal` réconciliée des deux sources :

| Nutri-Score | Seuil calorique (kcal/100g) |
|---|---|
| A | < 80 |
| B | 80 – 159 |
| C | 160 – 269 |
| D | 270 – 399 |
| E | ≥ 400 |

Ce score simplifié est volontairement basé uniquement sur les calories pour garantir une **couverture maximale** (les autres nutriments ont un taux de null élevé) et permettre des filtres à facettes immédiatement exploitables côté front-end.

### 4.4 Catégorisation du temps de cuisson

```python
F.when(F.col("cook_minutes") <= 30, "rapide")
 .when(F.col("cook_minutes") <= 60, "moyen")
 .when(F.col("cook_minutes").isNotNull(), "long")
 .otherwise("inconnu")
```

Cette colonne dérivée `cook_time_category` évite au moteur de recherche d'effectuer des comparaisons numériques sur chaque requête. Le filtrage sur une valeur catégorielle (`"rapide"`) est significativement plus rapide que `WHERE cook_minutes <= 30`.

---

## 5. Décisions d'optimisation Spark

### 5.1 Staging intermédiaire en Parquet

Écrire les données en Parquet entre les deux phases permet :
- De **briser le plan d'exécution Spark** : sans ce checkpoint, Spark tente de composer toutes les transformations en un seul job, ce qui peut produire des plans trop larges et mal optimisés.
- De **réduire la taille des données lues** en Phase 2 : Parquet est un format colonnaire compressé. Seules les colonnes effectivement utilisées sont lues depuis le disque.
- De **faciliter le débogage** : chaque dataset intermédiaire peut être inspecté indépendamment.

### 5.2 Contrôle du nombre de partitions (`N_PARTITIONS = 8`)

Par défaut, Spark crée souvent trop de partitions (200 par défaut pour les shuffles), ce qui génère des milliers de petits fichiers. Le pipeline fixe `N_PARTITIONS = 8` pour la table principale, ce qui :
- Correspond à un volume raisonnable par fichier pour un dataset de cette taille.
- Réduit le problème des **small files** qui dégradent les performances du metadata store (Databricks/HDFS).

### 5.3 `dropDuplicates` à plusieurs niveaux

Des `dropDuplicates` sont appliqués à deux moments distincts :
1. **En Phase 1** sur le CSV Kaggle (`title_norm`) : pour éviter qu'un titre dupliqué ne génère une explosion de lignes lors de la jointure.
2. **En Phase 2** sur `recipe_id` dans `df_final` et `df_nutrition_detail` : filet de sécurité pour absorber d'éventuels doublons résiduels produits par les jointures sur `title_norm` (clé non-unique par construction).

---

## 6. Modèle de données final

Le pipeline produit **3 tables Delta** distinctes, conçues pour des usages différents :

### Table 1 : `recipes_main`

Contient toutes les colonnes nécessaires à l'affichage d'une recette complète.

| Colonne | Type | Description |
|---|---|---|
| `recipe_id` | String | Identifiant unique (MIT) |
| `title` | String | Titre original |
| `description` | String | Description (Kaggle) |
| `instructions_text` | String | Instructions concaténées (`\|` comme séparateur) |
| `ingredients_raw` | Array[String] | Ingrédients bruts (layer1) |
| `ingredients_validated` | Array[String] | Ingrédients normalisés et validés |
| `n_ingredients_validated` | Integer | Compte des ingrédients valides |
| `n_steps` | Integer | Nombre d'étapes |
| `cook_minutes` | Integer | Durée totale de cuisson |
| `cook_time_category` | String | `rapide` / `moyen` / `long` / `inconnu` |
| `image_url` | String | URL de l'image principale |
| `image_urls` | Array[String] | Toutes les URLs d'images |
| `has_image` | Boolean | Flag de présence d'image |
| `source_url` | String | URL source originale |
| `energy_kcal` | Float | Calories per 100g (réconciliées) |
| `nutri_score` | String | Score nutritionnel simplifié (A–E) |
| `tags` | Array[String] | Tags Food.com |

**Partitionnement :** par `nutri_score` (5 valeurs → 5 dossiers × 8 partitions).

### Table 2 : `ingredients_index`

Table dénormalisée (1 ligne = 1 ingrédient × 1 recette), optimisée pour les recherches par ingrédient.

| Colonne | Type | Description |
|---|---|---|
| `recipe_id` | String | Référence à `recipes_main` |
| `title` | String | Titre de la recette |
| `nutri_score` | String | Pour le filtrage croisé |
| `image_url` | String | Pour l'affichage dans les résultats |
| `cook_time_category` | String | Pour le filtrage croisé |
| `ingredient` | String | Ingrédient normalisé (lowercase, trimmed) |

### Table 3 : `recipes_nutrition_detail`

Table complémentaire isolant les valeurs nutritionnelles détaillées, non chargées par défaut.

| Colonne | Type | Description |
|---|---|---|
| `recipe_id` | String | Référence à `recipes_main` |
| `fat_g` | Float | Lipides (g/100g) |
| `protein_g` | Float | Protéines (g/100g) |
| `salt_g` | Float | Sel (g/100g) |
| `saturates_g` | Float | Acides gras saturés (g/100g) |
| `sugars_g` | Float | Sucres (g/100g) |

---

## 7. Optimisations Databricks (Delta & Z-Order)

### Format Delta Lake

Les tables finales sont écrites au format **Delta** plutôt qu'en Parquet brut. Delta ajoute :
- Un **transaction log** (`_delta_log`) permettant des lectures snapshot consistantes et des mises à jour atomiques.
- La compatibilité avec les fonctionnalités Databricks d'optimisation (OPTIMIZE, Z-Order, Data Skipping).

### Z-Order Clustering

```sql
OPTIMIZE delta.`/recipes_main`              ZORDER BY (title)
OPTIMIZE delta.`/ingredients_index`         ZORDER BY (ingredient)
OPTIMIZE delta.`/recipes_nutrition_detail`  ZORDER BY (recipe_id)
```

**Principe du Z-Order :** Databricks réorganise physiquement les fichiers Parquet sous-jacents pour regrouper les lignes ayant des valeurs proches sur la colonne Z-Ordonnée. Lors d'une requête filtrant sur cette colonne, le moteur peut utiliser les **statistiques de colonnes** (min/max par fichier) pour **ignorer les fichiers ne pouvant pas contenir les résultats** — c'est le mécanisme de *Data Skipping*.

**Impact concret :**
- Une recherche `WHERE ingredient = 'tomate'` sur `ingredients_index` scanne seulement les fichiers contenant effectivement des ingrédients proches de "tomate" alphabétiquement.
- Sans Z-Order, Spark scannerait tous les fichiers de la table.

### Pourquoi `ingredients_index` n'est pas partitionné par `ingredient` ?

Un partitionnement par ingrédient créerait autant de dossiers qu'il y a d'ingrédients distincts (potentiellement des dizaines de milliers). Chaque dossier ne contiendrait que quelques lignes, produisant des **milliers de petits fichiers** — un anti-pattern bien documenté qui dégrade les performances du listing de métadonnées. Le Z-Order offre les mêmes gains de filtrage sans fragmenter le stockage.

---

## 8. Conception orientée usage final (UI/API)

L'ensemble du pipeline est pensé pour minimiser la latence des requêtes servies par une API de recherche de recettes. Les choix structurels suivants reflètent cet objectif :

**Séparation des tables par usage** : `recipes_main` contient tout ce qui est nécessaire à l'affichage d'une fiche recette. `ingredients_index` est la seule table interrogée lors d'une recherche par ingrédient. `recipes_nutrition_detail` n'est chargée qu'à la demande (vue détaillée). Aucune requête courante ne nécessite de scanner les trois tables simultanément.

**Colonnes calculées en amont** : `nutri_score`, `cook_time_category`, `has_image`, `n_ingredients_validated` sont toutes des valeurs dérivées calculées une fois à l'ingestion. Côté API, un filtre sur `nutri_score = 'A'` est une simple comparaison de chaînes, sans aucune logique métier à recalculer.

**Dénormalisation de `ingredients_index`** : La répétition de `title`, `image_url`, `nutri_score` et `cook_time_category` dans la table d'index est intentionnelle. Elle évite toute jointure au moment de la recherche : la liste des résultats peut être construite en interrogeant uniquement `ingredients_index`, sans jamais toucher `recipes_main`.

**Normalisation des ingrédients** (`F.lower(F.trim(...))`) à l'écriture dans `ingredients_index` : les requêtes de recherche par ingrédient n'ont ainsi pas besoin d'appliquer de transformations, ce qui maximise l'efficacité du Data Skipping Z-Order.