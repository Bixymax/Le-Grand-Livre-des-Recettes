![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![Plotly Dash](https://img.shields.io/badge/Dash_Plotly-0080FF?style=for-the-badge&logo=plotly&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00AEEF?style=for-the-badge&logo=databricks&logoColor=white)

# 📖 Le Grand Livre des Recettes : Pipeline Data & Dashboard Analytique

Un projet Data Full-Stack qui ingère, nettoie, enrichit, indexe et visualise plus d'un million de recettes de cuisine. Ce projet illustre la fusion de deux datasets majeurs (le dataset massif du **MIT** et celui de **Food.com via Kaggle**) en combinant le traitement Big Data (PySpark & Delta Lake), un moteur de recherche textuel ultra-rapide (DuckDB) et une interface utilisateur interactive (Dash & Plotly).

## 🚀 Fonctionnalités Principales

* **Pipeline ETL Big Data & Enrichissement :** Traitement distribué de fichiers JSON/CSV avec PySpark. Le dataset de base du MIT (+1 million de recettes) est nettoyé, normalisé, puis **enrichi par jointure** avec les données de Food.com issues de Kaggle pour récupérer les macronutriments, les temps de cuisson et les tags manquants.
* **Moteur de Recherche Intégré :** Indexation textuelle avancée (Full-Text Search) propulsée par DuckDB avec calcul de pertinence (algorithme BM25) pour une recherche instantanée parmi plus d'un million de lignes.
* **Scoring Nutritionnel :** Calcul dynamique d'un Nutri-Score simplifié (A à E) basé sur les kilocalories consolidées lors de l'enrichissement, et exposition des macronutriments (protéines, lipides, glucides).
* **Dashboard Interactif (Dash) :**
    * Recherche textuelle ultra-rapide de recettes.
    * Panneau de filtres globaux dynamiques (Nutri-Score, temps de cuisson, énergie).
    * Graphiques croisés interactifs (répartition nutritionnelle, scatter plots des macros, top ingrédients/tags).
    * Génération aléatoire de recettes avec récupération d'images à la volée.

---

## 🛠️ Stack Technique

* **Data Engineering :** PySpark, Delta Lake, Parquet
* **Base de Données & Recherche :** DuckDB (FTS extension)
* **Data Science / Analyse :** Pandas, NumPy
* **Frontend / Data Vision / Dashboard :** Plotly, Dash
* **Développement :** Python, Jupyter Notebooks

---

## 📂 Structure du Projet

```text
├── data/                       # Dossier contenant les données brutes (MIT JSONs, Kaggle CSV) et les bases générées
├── scripts/
│   ├── recipe_pipeline_final.ipynb   # ETL PySpark : Ingestion, jointure MIT x Kaggle, enrichissement et écriture Delta
│   ├── recipe_data_exploration.ipynb # Analyse exploratoire : Qualité des données, complétude après jointure, outliers
│   └── recipe_poc.ipynb              # Proof of Concept : Tests de recherche et optimisation avec Spark
├── dashboard/
│   ├── main.py                 # Point d'entrée de l'application Dash
│   ├──ingestion.py             # Script faisant le pont entre Delta Lake et DuckDB
│   └── app/
│       ├── layout.py               # Définition de la structure HTML et du CSS (Layout Dash)
│       ├── callbacks.py            # Logique applicative : interactivité, filtres, recherche
│       ├── charts.py               # Usines à graphiques Plotly (histogrammes, scatters, etc.)
│       ├── data.py                 # Connexion DuckDB et calcul des KPIs statiques initiaux
│       └── config.py               # Configuration UI (Palettes de couleurs, Polices)
├── requirements.txt
└── README.md
```

--- 

## ⚙️ Installation & Lancement

### 1. Prérequis
Assurez-vous d'avoir Python 3.8+ installé. Il est recommandé d'utiliser un environnement virtuel.

```bash
# Création et activation de l'environnement virtuel
python -m venv venv
source venv/bin/activate  # Sur Windows : venv\Scripts\activate

# Installation des dépendances
pip install requirements.txt
```

### 2. Éxecution du Pipeline ETL (PySpark)
Afin de générer les données propres pour l'application, vous devez d'abord exécuter le notebook principal.

1. Lancez `notebooks/recipe_pipeline_final.ipynb`.
2. Ce notebook va transformer les fichiers bruts, croiser les données du MIT et de Kaggle, et générer les tables optimisées dans `data/recipes_parquets/` (format Delta).

### 3. Ingestion dans DuckDB
Une fois les fichiers Parquet/Delta générés, transférez-les dans une base DuckDB locale et créez les index de recherche :

```bash
# Depuis la racine du projet
python app/ingestion.py
```

_Ce script va créer le fichier `recipes_catalog.duckdb` dans le dossier data/._

### 4. Lancement de l'Application Web
Démarrez le serveur Dash :

```bash
python app/main.py
```

## 📊 Détails de l'Architecture

### Phase 1 : Préparation & Enrichissement (Spark / Delta)
Les données proviennent de plusieurs couches hétérogènes (fichiers JSON imbriqués du MIT contenant les images et les étapes NLP, et fichiers CSV de Kaggle contenant les métriques de cuisson et de nutrition).
Le pipeline normalise agressivement les textes (retrait de la ponctuation, minuscules) pour créer une clé de jointure robuste et fiabiliser la fusion des datasets. Les arrays d'ingrédients sont pivotés (`explode_outer`) pour créer un index de recherche inversé optimisé avec la technologie **Z-Order** de Delta Lake, permettant un Data Skipping extrêmement performant.

### Phase 2 : Stockage Orienté Analyse (DuckDB)
Plutôt que d'interroger Spark en direct pour le dashboard web, les données finales sont avalées par **DuckDB**. Une vue dénormalisée `recipes` est créée, et l'extension `fts` (Full-Text Search) est configurée avec un stemmer français pour permettre des recherches partielles ultra-rapides (BM25) sur les titres et ingrédients depuis l'UI.

### Phase 3 : Visualisation (Dash)
Le dashboard utilise un store front-end (`dcc.Store`) pour gérer l'état global des filtres de l'utilisateur. Chaque clic sur un graphique (ex: cliquer sur la barre "Nutri-Score A") met à jour instantanément l'ensemble des autres visualisations grâce au Cross-Filtering, garantissant une exploration fluide du catalogue.

---

Projet réalisé dans le cadre d'une exploration Data Engineering & Analytics pour le cours de Big Data.