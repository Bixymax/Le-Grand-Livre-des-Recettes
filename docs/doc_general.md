# 📄 Rapport de Proof of Concept (PoC) : Moteur de Recherche de Recettes & Pipeline de Données

## 1. Introduction
Ce document détaille l'architecture de bout en bout d'un Proof of Concept (PoC) pour un moteur de recherche analytique de recettes. L'objectif est de démontrer comment la préparation en amont des données (ETL) combinée aux capacités d'un moteur de requêtage moderne permet de filtrer instantanément un vaste catalogue selon des critères croisés complexes (nom, ingrédients, temps, nutrition).

## 2. Pipeline ETL : La Fondation du PoC
Pour garantir des temps de réponse optimaux lors de la recherche, la complexité a été déportée lors de la phase de préparation des données. Le pipeline s'articule autour de trois axes majeurs :

- **Assemblage Résilient (Left Joins)** : La table de base (layer1) est jointe avec diverses sources (Kaggle, données nutritionnelles, détails d'ingrédients) via des jointures. J'ai réalisé le projet de cette façon afin d'avoir davantages de données sur chaque recette.

- **Enrichissement et Nettoyage** :  
  Création de variables métiers calculées, telles que la cook_time_category (rapide, moyen, long) ou le comptage d'ingrédients validés.

- **Stratégie de Fallback (Coalesce)** :  
  Pour maximiser la couverture des données, l'énergie en kcal priorise la source principale, mais bascule automatiquement sur les données Kaggle si la première est vide. Le Nutri-Score est ensuite calculé dynamiquement sur cette donnée fiabilisée.

- **Dédoublonnage** :  
  Une étape de sécurité (`dropDuplicates` sur `recipe_id`) est appliquée à la fin de la modélisation pour éviter tout produit cartésien accidentel issu des jointures textuelles.

## 3. Architecture du Modèle de Données (Delta Lake)
Le système déverse les données nettoyées dans trois tables Delta physiques, avec des stratégies d'écriture spécifiques au cas d'usage :

- **recipes_main** :  
  Table principale partitionnée physiquement par `nutri_score`. Les recherches filtrant par Nutri-Score (très fréquentes dans ce PoC) ignoreront ainsi des dossiers entiers de données, accélérant drastiquement la lecture.

- **ingredients_index** :  
  Une table d'indexation inversée générée via un `explode` du tableau des ingrédients.  
  **Choix critique** : Cette table est écrite sans partitionnement. Partitionner par ingrédient créerait des milliers de micro-fichiers, ce qui dégraderait sévèrement les performances du système de fichiers (problème des small files).

- **recipes_nutrition_detail** :  
  Informations diététiques avancées, stockées séparément pour éviter d'alourdir la table principale.

## 4. Choix Technologiques

- **Apache Spark (PySpark)** :  
  Utilisé pour sa capacité à gérer les transformations complexes (`joins`, `explode`) et à distribuer le calcul. Le PoC exploite ses capacités d'optimisation à l'exécution, comme le Broadcast Join (seuil de 200 Mo), qui évite le shuffle réseau lors de la jointure avec la table d'index des ingrédients.

- **Delta Lake** :  
  Apporte les transactions ACID, mais surtout des fonctionnalités d'optimisation physique du stockage (Data Skipping, Z-Order) indispensables pour la rapidité du PoC.

## 5. Stratégies d'Optimisation : Z-Order et Requêtes
L'excellence des performances de ce PoC repose sur la synergie entre la préparation physique des fichiers (Z-Order) et la façon dont Spark exécute les requêtes (Predicate Pushdown) :

### Le choix stratégique du Z-Order
L'application de la commande `OPTIMIZE ... ZORDER BY` est l'une des optimisations les plus fortes de ce pipeline. Le Z-Ordering réorganise physiquement les données à l'intérieur des fichiers Parquet (sous le capot de Delta) pour colocaliser les informations similaires.

- **ZORDER BY (title) sur recipes_main** :  
  Les recettes ayant des noms alphabétiquement proches sont stockées ensemble. Lors de la fonction `search_by_name("pasta")`, le moteur Delta Lake lit les métadonnées des fichiers et "saute" (Data Skipping) tous les fichiers qui ne contiennent pas cette plage de lettres.

- **ZORDER BY (ingredient) sur ingredients_index** :  
  C'est le cœur du PoC. Lors d'une recherche `search_by_ingredient("tomato")`, cette optimisation permet de trouver instantanément les `recipe_id` associés, transformant un scan complet et lent en une recherche ciblée ultra-rapide.

- **ZORDER BY (recipe_id) sur recipes_nutrition_detail** :  
  Accélère la construction de la "Vue Master" en alignant les clés de jointure.

### Exécution optimisée côté PoC

- **Predicate Pushdown** :  
  Dans la recherche avancée (`search_advanced`), les filtres (temps max, nutri-score) sont appliqués avant de joindre l'index des ingrédients. Le moteur bénéficie ainsi directement du partitionnement et du Z-Order préparés lors de l'ETL.

- **Court-circuitage (`.limit()`)** :  
  Lors de l'affichage d'une recette unitaire, l'arrêt prématuré de la requête évite le gaspillage de ressources de calcul.

## 6. Conclusion
Ce projet démontre qu'un moteur de recherche performant ne repose pas uniquement sur le code de l'interface de requêtage, mais sur une préparation minutieuse des données. L'utilisation stratégique du partitionnement par Nutri-score, de l'éclatement des ingrédients sans partitionnement abusif, et du Z-Ordering sur les colonnes massivement requêtées (titre, ingrédient), permet à ce PoC d'offrir des temps de réponse interactifs, tout en étant prêt à absorber des téraoctets de données.