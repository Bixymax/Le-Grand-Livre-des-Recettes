# Documentation Technique — PoC Moteur de Recherche de Recettes

> **Contexte :** Ce notebook est un Proof of Concept (PoC) local démontrant la faisabilité du moteur de recherche de recettes à partir des tables Delta produites par le pipeline de transformation. Il valide que l'architecture de données choisie permet des requêtes expressives, performantes et composables sur un dataset de plus d'un million de recettes.

---

## Table des Matières

1. [Objectif du PoC](#1-objectif-du-poc)
2. [Configuration Spark locale](#2-configuration-spark-locale)
3. [Stratégie de chargement des données](#3-stratégie-de-chargement-des-données)
4. [Vue Master matérialisée](#4-vue-master-matérialisée)
5. [Les 4 modes de recherche](#5-les-4-modes-de-recherche)
6. [Patterns de performance démontrés](#6-patterns-de-performance-démontrés)

---

## 1. Objectif du PoC

Ce notebook ne vise pas la production. Son rôle est de **valider trois hypothèses** posées lors de la conception du pipeline :

1. **Les tables Delta produites sont interrogeables avec des temps de réponse acceptables** sans infrastructure dédiée (Databricks, cluster multi-nœuds).
2. **L'index d'ingrédients dénormalisé** (`ingredients_index`) rend les recherches par ingrédient efficaces et simples à implémenter.
3. **Les filtres composés** (nom + ingrédient + temps + nutri-score) peuvent être exprimés proprement en PySpark et bénéficient des optimisations Delta (Data Skipping, predicate pushdown).

Le PoC tourne en **Spark local** (mode `local[*]`), ce qui signifie que toutes les validations de performance sont en réalité des lower bounds : les mêmes requêtes seront plus rapides sur un cluster distribué.

---

## 2. Configuration Spark locale

### Support Delta Lake

```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

Ces deux lignes sont **obligatoires** pour lire des tables au format Delta en local. Sans elles, Spark ne reconnaît pas le `_delta_log` et tente de lire les fichiers Parquet sous-jacents bruts, perdant ainsi toutes les métadonnées de partitionnement et de Z-Order.

### Broadcast join automatique

```python
.config("spark.sql.autoBroadcastJoinThreshold", "200m")
```

Le seuil de broadcast est monté à 200 Mo (vs 10 Mo par défaut). Cela permet à Spark d'automatiquement choisir un **broadcast hash join** lorsque `df_index` (la table d'ingrédients) tient en mémoire. Un broadcast join élimine le shuffle réseau : la petite table est répliquée sur tous les nœuds, et la jointure devient une opération locale (map-side join). Sur un cluster, c'est la différence entre quelques secondes et plusieurs dizaines de secondes.

### Allocation mémoire

```python
.config("spark.executor.memory", "3g")  # Nœuds de calcul
.config("spark.driver.memory", "1g")    # Chef d'orchestre
```

En mode local, driver et executor partagent la même JVM. Le ratio 3g/1g reflète la nature du workload : la majorité de la mémoire est consommée par les shuffles et le cache de `df_index`, pas par le driver qui n'agrège que les résultats finaux.

---

## 3. Stratégie de chargement des données

### Lecture différée vs cache explicite

```python
df_main      = spark.read.format("delta").load(...)   # Lazy — rien n'est lu ici
df_index     = spark.read.format("delta").load(...).cache()  # Cache après premier scan
df_nutrition = spark.read.format("delta").load(...)   # Lazy
```

Le comportement est délibérément asymétrique :

- `df_main` et `df_nutrition` sont **lazy** : Spark ne les lit que lorsqu'une action (`collect`, `toPandas`, `count`) est déclenchée. Cette approche préserve la mémoire et laisse l'optimiseur composer les filtres avant de toucher le disque.

- `df_index` est **mis en cache** explicitement via `.cache()` suivi de `.count()` (qui force la matérialisation). La table d'ingrédients est la plus consultée du PoC — elle est sollicitée à chaque recherche par ingrédient ou recherche avancée. La maintenir en mémoire évite de la relire depuis le disque à chaque requête.

> **Note :** `.cache()` seul ne déclenche pas la mise en cache. En Spark, le cache est lazy : il est réellement exécuté à la première action suivante, ici le `.count()`.

---

## 4. Vue Master matérialisée

### Pourquoi matérialiser plutôt que laisser une vue logique ?

```python
df_master_logic = df_main.join(df_nutrition, on="recipe_id", how="left")
df_master_logic.write.format("delta").mode("overwrite").save(MASTER_PATH)
df_master = spark.read.format("delta").load(MASTER_PATH)
```

Une alternative aurait été de garder `df_master_logic` comme variable Python et de l'utiliser directement. Cela aurait fonctionné, mais avec un coût : **la jointure entre `df_main` et `df_nutrition` aurait été recalculée à chaque requête**.

En matérialisant la vue dans une nouvelle table Delta, la jointure est exécutée une seule fois. Les requêtes suivantes lisent une table plate, sans jointure à recalculer. C'est le principe de la **vue matérialisée** (materialized view), appliqué manuellement ici faute d'un catalogue Databricks disponible en local.

La contrepartie est l'occupation disque supplémentaire et la nécessité de re-matérialiser si `df_main` ou `df_nutrition` sont mis à jour.

---

## 5. Les 4 modes de recherche

### 5.1 Recherche par nom (`search_by_name`)

```python
df_main.filter(col("title").ilike(f"%{query}%"))
```

`ilike` est la version insensible à la casse de `like`. Le pattern `%query%` recherche le terme n'importe où dans le titre. Cette approche est simple et suffisante pour un PoC, mais ne gère pas les fautes de frappe ni les synonymes — des limites attendues à ce stade.

La requête opère uniquement sur `df_main` et projette 5 colonnes seulement (`recipe_id`, `title`, `cook_minutes`, `nutri_score`, `image_url`), ce qui minimise les données transférées depuis le disque grâce à la **projection pushdown** de Parquet.

### 5.2 Recherche par ingrédient (`search_by_ingredient`)

```python
recipe_ids = df_index.filter(col("ingredient").ilike(f"%{ingredient}%")).select("recipe_id").distinct()
df_master.join(recipe_ids, on="recipe_id")
```

C'est la démonstration centrale du PoC. La stratégie en deux étapes est clé :

1. **Filtrage sur `df_index`** : on extrait uniquement les `recipe_id` correspondant à l'ingrédient. `df_index` est en cache, ce scan est purement mémoire.
2. **Jointure sur `df_master`** : on enrichit avec les détails de la recette. La jointure porte sur un ensemble réduit d'IDs, pas sur la table complète.

Le `df_index` tient dans les 200 Mo configurés, Spark transforme automatiquement cette jointure en broadcast hash join, éliminant tout shuffle.

### 5.3 Recherche avancée (`search_advanced`)

```python
def search_advanced(name=None, ingredient=None, max_time=None, nutri_score=None):
    df = df_master
    if name:      df = df.filter(col("title").ilike(...))
    if max_time:  df = df.filter(col("cook_minutes") <= max_time)
    if nutri_score: df = df.filter(col("nutri_score") == nutri_score)
    if ingredient:
        recipe_ids = df_index.filter(...).select("recipe_id").distinct()
        df = df.join(recipe_ids, on="recipe_id")
```

Plusieurs décisions de conception méritent d'être soulignées :

**Filtres avant jointure.** Les filtres sur `name`, `max_time` et `nutri_score` sont appliqués sur `df_master` *avant* la jointure avec `df_index`. Spark (avec son optimiseur Catalyst) bénéficie du **predicate pushdown** : les filtres sont poussés au plus proche de la lecture disque, réduisant le volume de données avant toute jointure.

**La jointure d'ingrédient en dernier.** Intentionnellement positionnée après tous les autres filtres, la jointure avec `df_index` opère sur un `df` déjà filtré. Moins de lignes côté `df_master` = jointure moins coûteuse.

**Paramètres optionnels (`None` par défaut).** Tous les filtres sont optionnels et indépendants. La fonction est un **query builder** : on accumule des transformations Spark sans les exécuter (lazy), et le plan d'exécution final est optimisé globalement par Catalyst avant le premier accès disque.

### 5.4 Affichage d'une recette complète (`show_recipe`)

```python
df_master.filter(col("recipe_id") == recipe_id).limit(1).collect()
```

Le `.limit(1)` combiné à un filtre sur une clé primaire est un pattern important. Sur une table partitionnée et Z-Ordonnée par `recipe_id`, Delta peut identifier le fichier exact contenant cet ID et s'arrêter dès le premier match — sans scanner la table entière. Le `.limit(1)` signale à Spark qu'il peut court-circuiter l'exécution dès qu'une ligne est trouvée.

---

## 6. Patterns de performance démontrés

Le PoC illustre concrètement les optimisations suivantes :

**Predicate Pushdown** — Les filtres PySpark (`.filter(...)`) sont traduits par Catalyst en prédicats poussés directement au niveau de la lecture Parquet. Seules les lignes satisfaisant les conditions sont chargées en mémoire ; les autres ne quittent jamais le disque.

**Projection Pushdown** — Les `.select(...)` explicites en fin de requête réduisent le nombre de colonnes lues depuis les fichiers Parquet colonnaires. Lire 5 colonnes sur 20 peut diviser le volume d'I/O par 4.

**Broadcast Hash Join** — Grâce à `autoBroadcastJoinThreshold = 200m` et au cache de `df_index`, les jointures sur la table d'ingrédients évitent tout shuffle réseau.

**Delta Data Skipping** — Pour les filtres sur `nutri_score` (colonne de partitionnement) ou `recipe_id` / `ingredient` (colonnes Z-Ordonnées dans le pipeline), Delta consulte les statistiques min/max stockées dans le `_delta_log` et ignore les fichiers ne pouvant pas contenir les résultats.

**Lazy Evaluation + Catalyst** — Aucune des transformations PySpark (`filter`, `join`, `select`) n'est exécutée immédiatement. Spark attend l'action terminale (`collect`, `toPandas`) pour construire un plan d'exécution global optimisé, potentiellement différent de l'ordre dans lequel les transformations ont été écrites.