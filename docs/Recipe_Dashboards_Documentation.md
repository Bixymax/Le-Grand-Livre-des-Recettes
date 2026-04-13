# Documentation Technique — Dashboard « Le Grand Livre des Recettes »

> **Contexte :** Cette application est un dashboard analytique interactif construit avec Dash et Plotly, servi par un moteur de requête DuckDB. Elle expose le catalogue de recettes produit par le pipeline de transformation sous forme de visualisations cross-filtrables, d'une recherche plein texte et d'une fiche recette dynamique.

---

## Table des Matières

1. [Architecture générale](#1-architecture-générale)
2. [Pipeline d'ingestion DuckDB (`ingestion.py`)](#2-pipeline-dingestion-duckdb-ingestionpy)
3. [Couche de données (`data.py`)](#3-couche-de-données-datapy)
4. [Système de configuration graphique (`config.py`)](#4-système-de-configuration-graphique-configpy)
5. [Génération des graphiques (`charts.py`)](#5-génération-des-graphiques-chartspy)
6. [Structure du layout (`layout.py`)](#6-structure-du-layout-layoutpy)
7. [Système de callbacks et interactivité (`callbacks.py`)](#7-système-de-callbacks-et-interactivité-callbackspy)
8. [Point d'entrée (`main.py`)](#8-point-dentrée-mainpy)

---

## 1. Architecture générale

L'application est organisée selon une séparation stricte des responsabilités en 5 couches :

```
dashboard/
├── app/
│   ├── __init__.py
│   ├── callbacks.py   ← Réactivité : tous les Input/Output/State Dash
│   ├── charts.py      ← Génération des figures Plotly (SQL → DataFrame → Figure)
│   ├── config.py      ← Palette, couleurs Nutri-Score, thème PLOT_LAYOUT
│   ├── data.py        ← Connexion DuckDB unique, constantes globales pré-calculées
│   └── layout.py      ← Structure HTML statique, composants Dash, KPIs au démarrage
├── assets/
│   └── style.css
├── dashboard_schema.png
├── db_schema.txt
├── ingestion.py       ← Script standalone : Delta → DuckDB (exécuté une seule fois)
├── main.py            ← Point d'entrée : instanciation Dash, montage layout + callbacks
└── Sans-titre-2026-04-03-0843.excalidraw
```

Le flux de données à l'exécution est le suivant : l'utilisateur interagit avec un composant Dash (filtre, clic sur un graphique, saisie de recherche), un callback est déclenché, il appelle une fonction de `charts.py` ou exécute directement une requête SQL via `data.con`, et le résultat est renvoyé comme `Output` vers le composant cible dans le layout.

---

## 2. Pipeline d'ingestion DuckDB (`ingestion.py`)

### Rôle

Ce script est le **pont entre le pipeline Spark** (qui produit des tables Delta partitionnées) **et le dashboard** (qui a besoin d'un accès SQL rapide en lecture). Il est exécuté une seule fois, ou à chaque mise à jour des données sources.

### Étape 1 — Lecture des tables Delta avec `delta_scan`

```python
con.execute("INSTALL delta; LOAD delta;")
con.execute(f"CREATE TABLE recipes_main AS SELECT * FROM delta_scan('{main_path}')")
```

DuckDB dispose d'une extension native `delta` qui lit directement les fichiers Parquet d'une table Delta, en respectant le `_delta_log` pour ne lire que les fichiers actifs (non supprimés par des opérations OPTIMIZE ou VACUUM précédentes). Cela évite d'exporter les données dans un format intermédiaire : Delta → DuckDB en une seule commande.

### Étape 2 — Création des index SQL

```python
con.execute("CREATE UNIQUE INDEX idx_main_recipe_id ON recipes_main(recipe_id);")
con.execute("CREATE INDEX idx_nutrition_recipe_id ON recipes_nutrition(recipe_id);")
```

Sur un million de lignes, une recherche par `recipe_id` sans index est un **full scan** O(n). Un index B-tree la réduit à O(log n). L'index sur `recipe_id` dans `recipes_nutrition` accélère également la jointure `LEFT JOIN ... ON recipe_id` effectuée dans de nombreuses requêtes des callbacks.

### Étape 3 — Vue analytique matérialisée comme VIEW

```python
con.execute("""
    CREATE VIEW recipes AS
    SELECT m.*, n.fat_g, n.protein_g, n.salt_g, n.sugars_g, n.saturates_g
    FROM recipes_main m LEFT JOIN recipes_nutrition n ON m.recipe_id = n.recipe_id
""")
```

La vue `recipes` est une **vue logique** (non matérialisée), pas une table. Elle n'occupe pas d'espace disque supplémentaire : DuckDB réécrit les requêtes qui la ciblent en leur substituant la définition de la vue à la volée. Elle sert de raccourci pour les requêtes qui ont besoin des colonnes des deux tables sans avoir à répéter la jointure à chaque fois.

### Étape 4 — Index Full-Text Search (FTS) avec BM25

```python
con.execute("""
    PRAGMA create_fts_index(
        'recipes_main', 'recipe_id', 'title', 'ingredients_validated',
        stemmer='french', stopwords='none', lower=1, strip_accents=1, overwrite=1
    );
""")
```

C'est l'étape la plus structurante pour la fonction de recherche. DuckDB construit un **index inversé BM25** (Best Match 25) sur les colonnes `title` et `ingredients_validated`. BM25 est l'algorithme de scoring de pertinence utilisé par la majorité des moteurs de recherche modernes (Elasticsearch, Solr, Lucene) : il pondère les termes par leur fréquence dans le document et leur rareté dans le corpus, produisant un score de pertinence plus fin qu'une simple recherche `LIKE`.

Les paramètres retenus méritent d'être détaillés :
- `stemmer='french'` : la racinisation française réduit les mots à leur radical (« tomates » → « tomat »), permettant de retrouver une recette contenant « tomates » en cherchant « tomate ».
- `strip_accents=1` : normalise les accents pour que « réduction » et « reduction » soient équivalents.
- `lower=1` : insensibilité à la casse.
- `stopwords='none'` : les mots vides (« le », « de », « avec ») ne sont pas filtrés, ce qui est pertinent pour des titres de recettes très courts où chaque mot compte.

---

## 3. Couche de données (`data.py`)

### Connexion unique en lecture seule

```python
con = duckdb.connect(DB_PATH, read_only=True)
```

Une **connexion DuckDB unique et partagée** est créée au démarrage du module. Le mode `read_only=True` est important pour deux raisons : il empêche toute modification accidentelle de la base pendant l'exécution du dashboard, et il permet à DuckDB d'autoriser des connexions concurrentes (plusieurs threads du serveur Dash peuvent lire simultanément).

### Pré-calcul des KPIs au démarrage

```python
_stats = con.cursor().execute("""
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE has_image = true) AS with_image,
        ...
    FROM recipes_main
""").fetchone()
```

Les statistiques globales affichées dans la bannière (nombre total de recettes, pourcentage rapides, kcal moyennes...) sont calculées **une seule fois au démarrage** du module Python, pas à chaque requête. Le résultat est stocké dans des constantes Python (`TOTAL_RECIPES`, `AVG_KCAL`, etc.) exportées vers `layout.py`. Cela évite d'exécuter ces agrégations coûteuses sur l'ensemble du dataset à chaque rechargement de page.

### Introspection du schéma pour la compatibilité

```python
_main_cols = {row[0] for row in con.execute("DESCRIBE recipes_main").fetchall()}
_image_urls_select = (
    "m.image_urls" if "image_urls" in _main_cols else "NULL::VARCHAR[] AS image_urls"
)
```

Ce pattern vérifie à l'exécution si la colonne `image_urls` existe dans la table, et adapte les requêtes SQL en conséquence. C'est une mesure de robustesse : si le pipeline de données est rejoué sans cette colonne (par exemple lors d'un test sur un dataset partiel), le dashboard ne crash pas — il renvoie simplement `NULL` à la place.

---

## 4. Système de configuration graphique (`config.py`)

### Palette et couleurs Nutri-Score

```python
PALETTE = { "bg": "#FAFAF7", "accent1": "#E07B39", ... }
NUTRI_COLORS = { "A": "#3D7A5F", "B": "#6BAE6A", "C": "#F2C14E", "D": "#E07B39", "E": "#C94F4F" }
```

Toutes les couleurs sont centralisées dans un seul fichier. Quand un graphique grise les barres non-sélectionnées lors du cross-filtering, il référence `PALETTE["border"]` plutôt qu'une valeur hexadécimale en dur. Modifier la palette complète du dashboard se fait en un seul endroit.

Le code couleur du Nutri-Score suit la convention officielle (vert → orange → rouge), ce qui rend les visualisations auto-explicatives sans légende.

### Thème graphique global `PLOT_LAYOUT`

```python
PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="'Playfair Display', Georgia, serif", ...),
    margin=dict(l=10, r=10, t=30, b=10), showlegend=False,
)
```

`PLOT_LAYOUT` est un dictionnaire Python appliqué à **tous les graphiques** via `fig.update_layout(**PLOT_LAYOUT, ...)`. C'est l'équivalent d'un thème CSS pour les figures Plotly : fond transparent (pour s'intégrer dans les cards du layout), police cohérente, marges minimales. Chaque fonction de graphique peut ensuite **surcharger** des propriétés spécifiques après application du thème global, comme le montre `scatter_saturates_sugars` qui réactive `showlegend=True`.

---

## 5. Génération des graphiques (`charts.py`)

### `_build_where` — constructeur de clause SQL dynamique

```python
def _build_where(nutri_scores, cook_cats, kcal_min, kcal_max,
                 table_prefix="", base_clauses=None) -> tuple[str, list]:
```

C'est la pièce centrale de l'architecture cross-filtering. Plutôt que de dupliquer la logique de filtrage dans chacune des 8 fonctions de graphique, `_build_where` génère dynamiquement la clause `WHERE` et sa liste de paramètres en fonction des filtres actifs.

Le pattern de **paramètres positionnels** (`?`) plutôt que de l'interpolation de chaînes est fondamental ici :

```python
# ❌ Interpolation directe — risque d'injection SQL
f"WHERE nutri_score IN ('{','.join(nutri_scores)}')"

# ✅ Paramètres positionnels — DuckDB échappe les valeurs
f"WHERE nutri_score IN ({','.join(['?'] * len(nutri_scores))})"
params.extend(nutri_scores)
```

Le `table_prefix` gère le cas des requêtes avec jointures, où les colonnes doivent être préfixées par leur alias de table (ex: `m.nutri_score` vs `nutri_score`).

### Cross-filtering visuel — grisage des barres non-sélectionnées

```python
bar_colors = [
    NUTRI_COLORS.get(s, PALETTE["muted"]) if (not nutri_scores or s in nutri_scores) else PALETTE["border"]
    for s in df["score"]
]
```

Quand des filtres Nutri-Score sont actifs, les barres correspondant aux scores non-sélectionnés sont colorées avec `PALETTE["border"]` (gris clair) plutôt qu'avec leur couleur nominale. Ce feedback visuel indique à l'utilisateur quelles valeurs sont filtrées sans supprimer les barres du graphique — ce qui aurait déformé l'axe et rendu la comparaison impossible.

### Sampling pour le scatter plot

```python
... USING SAMPLE 2000 ROWS
```

Le scatter plot `saturates_g` vs `sugars_g` affiche des points individuels par recette. Afficher 1 million de points rendrait le graphique inutilisable (surcharge visuelle, performances navigateur dégradées). `USING SAMPLE 2000 ROWS` est la syntaxe DuckDB pour un **échantillonnage aléatoire au niveau SQL**, exécuté avant le transfert des données en mémoire Python. 2000 points est suffisant pour visualiser les corrélations et la distribution par Nutri-Score.

### Helper générique `_generic_top_chart`

```python
def _generic_top_chart(sql_field, color, title, ...) -> go.Figure:
    df = con.execute(f"""
        SELECT item, COUNT(*) AS freq
        FROM (SELECT UNNEST({sql_field}) AS item FROM recipes_main {where_str})
        WHERE item IS NOT NULL AND LENGTH(TRIM(item)) > 1
        GROUP BY item ORDER BY freq DESC LIMIT ?
    """, params).df()
```

Les graphiques « Top Ingrédients » et « Top Tags » partagent exactement la même structure : dépliage d'un array (`UNNEST`), comptage, tri, limitation. La factorisation dans `_generic_top_chart` évite la duplication et garantit que les deux graphiques se comportent identiquement face aux filtres.

`UNNEST` est l'opérateur DuckDB qui transforme un array stocké dans une cellule en autant de lignes que l'array a d'éléments — l'équivalent de `F.explode` en Spark, mais ici exécuté directement en SQL sans avoir besoin de matérialiser une table dénormalisée.

---

## 6. Structure du layout (`layout.py`)

### Séparation layout / données

`layout.py` ne contient **aucune logique métier ni requête SQL**. Il importe les constantes pré-calculées de `data.py` (KPIs) et les expose comme du texte statique dans les composants HTML. Toute la partie dynamique (graphiques, fiches recette) est représentée par des composants avec `figure={}` ou `children=[]` vides, destinés à être remplis par les callbacks.

### `dcc.Store` — gestion d'état côté client

```python
dcc.Store(id="store-filters", data={"nutri_scores": [], "cook_cats": [], "kcal_min": 0, "kcal_max": 3500})
dcc.Store(id="store-selected-recipe-id", data=None)
dcc.Store(id="store-recipe-image-urls")
```

Les `dcc.Store` sont des composants invisibles qui stockent des données JSON dans le navigateur (mémoire client). Dans Dash, l'état partagé entre plusieurs callbacks ne peut pas être stocké dans des variables Python globales (risque de conflits entre sessions concurrentes). Les `Store` résolvent ce problème : les filtres actifs sont stockés dans `store-filters` et consultés par tous les callbacks qui en ont besoin, sans passer par le serveur.

### `dcc.Interval` pour le chargement différé

```python
dcc.Interval(id="init-interval", interval=1, n_intervals=0, max_intervals=1)
```

Les graphiques sont initialisés vides (`figure={}`) pour que la page HTML se charge instantanément. Un `Interval` se déclenche **1 ms après le chargement** de la page (`max_intervals=1` garantit qu'il ne se déclenche qu'une seule fois) et provoque le callback `load_all_charts`, qui exécute les 8 requêtes SQL et peuple tous les graphiques. L'utilisateur voit la page s'afficher immédiatement plutôt d'attendre le calcul des 8 figures côté serveur avant le premier rendu.

### Composants de filtres

Les filtres utilisent `dcc.Checklist` pour le Nutri-Score et le temps de cuisson, et `dcc.RangeSlider` pour la plage calorique. Ces trois composants servent d'`Input` à un callback central (`update_filter_store`) qui consolide leurs valeurs dans `store-filters`. C'est le `store-filters` qui est ensuite l'`Input` de `load_all_charts` — découplant ainsi les widgets de filtres de la logique de rafraîchissement des graphiques.

---

## 7. Système de callbacks et interactivité (`callbacks.py`)

### Architecture du flux de données

```
[dcc.Checklist / RangeSlider]
         │ Input
         ▼
  update_filter_store  →  store-filters  (State partagé)
                                │ Input
                                ▼
                        load_all_charts  →  8 × dcc.Graph figures
                        
[search-input / btn-search]
         │ Input
         ▼
  search_recipes  →  search-results (liste HTML)
         │
  [clic sur un résultat]
         │ Input
         ▼
  store_clicked_recipe  →  store-selected-recipe-id
         │ Input
         ▼
  update_recipe_panel  →  [titre, instructions, ingrédients, placeholder image]
         │ Output
  store-recipe-image-urls  →  resolve_recipe_image  →  [image résolue]
```

### Callback de filtres — consolidation dans un Store

```python
@app.callback(
    Output("store-filters", "data"),
    Input("filter-nutri", "value"),
    Input("filter-cook", "value"),
    Input("filter-kcal", "value"),
    Input("btn-reset-filters", "n_clicks"),
)
def update_filter_store(nutri, cook, kcal, reset_clicks):
```

Un seul callback collecte les trois widgets de filtres et les sérialise dans `store-filters`. Ce design évite de devoir connecter chaque widget directement à chaque graphique (ce qui produirait 3 inputs × 8 graphiques = 24 dépendances), et permet à n'importe quel callback futur de lire l'état des filtres sans modification de l'existant.

### Recherche FTS avec BM25

```python
df_res = con.execute("""
    SELECT recipe_id, title, nutri_score, cook_time_category,
           fts_main_recipes_main.match_bm25(recipe_id, ?) AS score
    FROM recipes_main
    WHERE fts_main_recipes_main.match_bm25(recipe_id, ?) IS NOT NULL
    ORDER BY score DESC LIMIT 8
""", [query.strip(), query.strip()]).df()
```

`fts_main_recipes_main.match_bm25` est la fonction générée par DuckDB lors de la création de l'index FTS dans `ingestion.py`. Elle retourne un score BM25 numérique pour chaque document, ou `NULL` si le document ne contient aucun des termes recherchés. Le `WHERE ... IS NOT NULL` filtre donc automatiquement les non-correspondances, et `ORDER BY score DESC` trie par pertinence décroissante.

### Résolution d'image asynchrone en deux callbacks

Le chargement d'une recette est volontairement découpé en deux callbacks séquentiels :

**Callback 1 — `update_recipe_panel`** : Récupère toutes les données textuelles (titre, instructions, ingrédients, nutri-score) depuis DuckDB et les affiche immédiatement. En parallèle, il écrit les URLs d'images dans `store-recipe-image-urls` et affiche un placeholder « ⏳ Chargement de l'image… ».

**Callback 2 — `resolve_recipe_image`** : Déclenché par la mise à jour de `store-recipe-image-urls`, il itère sur les URLs candidates et tente un `HTTP GET` avec un timeout court (2 secondes) pour valider que chaque URL répond avec un Content-Type image.

```python
def _find_valid_image_url(image_url, image_urls) -> str:
    for url in candidates:
        req = urlopen(url, timeout=2)
        if req.status == 200 and "image" in req.headers.get("Content-Type", ""):
            return url
```

Ce découplage est crucial : la validation HTTP d'une URL externe peut prendre plusieurs secondes si l'URL est morte ou le serveur lent. Sans cette séparation, l'utilisateur attendrait la résolution de l'image avant de voir le titre et les ingrédients, ce qui dégraderait fortement la perception de la réactivité.

### Pattern de callbacks sur composants dynamiques

```python
@app.callback(
    Output("store-selected-recipe-id", "data"),
    Input({"type": "search-result-item", "index": dash.ALL}, "n_clicks"),
    State({"type": "search-result-item", "index": dash.ALL}, "id"),
)
def store_clicked_recipe(n_clicks_list, ids):
    triggered_id = dash.callback_context.triggered[0]["prop_id"]
    id_dict = json.loads(triggered_id.replace(".n_clicks", ""))
    return id_dict.get("index")
```

Les résultats de recherche sont des composants générés dynamiquement avec des IDs de la forme `{"type": "search-result-item", "index": "<recipe_id>"}`. Dash supporte le **pattern matching** sur les IDs via `dash.ALL`, qui abonne le callback à n'importe quel composant correspondant au pattern, même ceux créés après le chargement initial de la page. L'identifiant de la recette cliquée est extrait depuis le `prop_id` du contexte de callback, qui contient l'ID JSON du composant ayant déclenché l'événement.

---

## 8. Point d'entrée (`main.py`)

```python
app = dash.Dash(
    __name__,
    external_stylesheets=[GOOGLE_FONTS],
    title="Le Grand Livre des Recettes",
)
app.layout = build_layout()
register_callbacks(app)
```

Le point d'entrée est volontairement minimal. `build_layout()` et `register_callbacks(app)` encapsulent toute la complexité dans leurs modules respectifs. L'objet `app` est l'unique lien entre les deux — `register_callbacks` reçoit l'instance `app` pour y enregistrer les callbacks via les décorateurs `@app.callback`.

La police `Playfair Display` est chargée depuis Google Fonts via `external_stylesheets`, cohérente avec le thème typographique défini dans `config.PLOT_LAYOUT` et appliqué à toutes les figures Plotly.