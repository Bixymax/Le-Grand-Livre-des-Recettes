"""
Tests d'intégration — pipeline complet sur mini-fixtures (3 recettes).

Ces tests vérifient :
  1. Staging : les tables raw_* sont correctement peuplées par dlt.
  2. Tables finales : recipes_main, ingredients_index, recipes_nutrition_detail.
  3. Sémantique énergie : nutri_score calculé sur mit_energy_kcal uniquement.
  4. Qualité des données : pas de recipe_id NULL, cook_time_category valide, etc.
  5. Jointures : recettes avec/sans match MIT↔Kaggle sont correctement traitées.

La fixture `pipeline_db` (conftest.py) tourne une seule fois pour toute la session.
Les tests eux-mêmes ne font que des lectures (DuckDB read_only=True).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _query(pipeline_db: Path, sql: str):
    with duckdb.connect(str(pipeline_db), read_only=True) as con:
        return con.execute(sql).fetchall()


def _scalar(pipeline_db: Path, sql: str):
    return _query(pipeline_db, sql)[0][0]


# ---------------------------------------------------------------------------
# 1. Tables staging
# ---------------------------------------------------------------------------

class TestStagingTables:

    def test_layer1_row_count(self, pipeline_db):
        """Les 3 recettes MIT doivent être chargées dans raw_layer1."""
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.raw_layer1")
        assert n == 3

    def test_layer2_row_count(self, pipeline_db):
        """Seules 2 recettes ont des images dans le fixture."""
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.raw_layer2")
        assert n == 2

    def test_det_ingrs_row_count(self, pipeline_db):
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.raw_det_ingrs")
        assert n == 3

    def test_kaggle_row_count(self, pipeline_db):
        """Seule 1 recette Kaggle dans le fixture (chocolate cake)."""
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.raw_kaggle")
        assert n == 1

    def test_nutrition_row_count(self, pipeline_db):
        """2 recettes ont des données nutritionnelles MIT (choc. cake + apple pie)."""
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.raw_nutrition")
        assert n == 2


# ---------------------------------------------------------------------------
# 2. Table recipes_main
# ---------------------------------------------------------------------------

class TestRecipesMain:

    def test_total_count(self, pipeline_db):
        """recipes_main doit avoir exactement 3 lignes (1 par recette MIT)."""
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.recipes_main")
        assert n == 3

    def test_no_null_recipe_id(self, pipeline_db):
        nulls = _scalar(
            pipeline_db,
            "SELECT COUNT(*) FROM recipes.recipes_main WHERE recipe_id IS NULL",
        )
        assert nulls == 0

    def test_recipe_id_unique(self, pipeline_db):
        total = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.recipes_main")
        distinct = _scalar(pipeline_db, "SELECT COUNT(DISTINCT recipe_id) FROM recipes.recipes_main")
        assert total == distinct

    def test_has_image_flags(self, pipeline_db):
        """chocolate cake et pasta bolognese ont des images ; apple pie non."""
        rows = {
            r[0]: r[1]
            for r in _query(
                pipeline_db,
                "SELECT recipe_id, has_image FROM recipes.recipes_main",
            )
        }
        assert rows["abc111"] is True,  "chocolate cake doit avoir has_image=True"
        assert rows["bcd222"] is True,  "pasta bolognese doit avoir has_image=True"
        assert rows["cde333"] is False, "apple pie doit avoir has_image=False"

    def test_cook_time_category_valid_values(self, pipeline_db):
        cats = {
            r[0]
            for r in _query(
                pipeline_db,
                "SELECT DISTINCT cook_time_category FROM recipes.recipes_main",
            )
        }
        assert cats.issubset({"rapide", "moyen", "long", "inconnu"})

    def test_chocolate_cake_cook_time(self, pipeline_db):
        """chocolate cake = 45 min → catégorie 'moyen'."""
        cat = _scalar(
            pipeline_db,
            "SELECT cook_time_category FROM recipes.recipes_main WHERE recipe_id = 'abc111'",
        )
        assert cat == "moyen"


# ---------------------------------------------------------------------------
# 3. Sémantique énergie — le cœur du bug corrigé
# ---------------------------------------------------------------------------

class TestEnergyUnits:

    def test_mit_and_kaggle_columns_exist(self, pipeline_db):
        """Les deux colonnes d'énergie doivent coexister sans COALESCE."""
        cols = {
            r[0]
            for r in _query(
                pipeline_db,
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'recipes_main'
                  AND table_schema = 'recipes'
                """,
            )
        }
        assert "mit_energy_kcal" in cols,    "mit_energy_kcal doit exister"
        assert "kaggle_energy_kcal" in cols,  "kaggle_energy_kcal doit exister"

    def test_energy_kcal_removed(self, pipeline_db):
        """La colonne COALESCE 'energy_kcal' ne doit plus exister."""
        cols = {
            r[0]
            for r in _query(
                pipeline_db,
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'recipes_main'
                  AND table_schema = 'recipes'
                """,
            )
        }
        assert "energy_kcal" not in cols, \
            "energy_kcal ne doit plus exister — elle mélangeait kcal/100g et kcal/portion"

    def test_nutri_score_null_when_no_mit_energy(self, pipeline_db):
        """
        pasta bolognese n'a pas de données MIT → mit_energy_kcal IS NULL
        → nutri_score doit être NULL (pas calculé sur les données Kaggle).
        """
        row = _query(
            pipeline_db,
            """
            SELECT mit_energy_kcal, kaggle_energy_kcal, nutri_score
            FROM recipes.recipes_main
            WHERE recipe_id = 'bcd222'
            """,
        )[0]
        mit_e, kaggle_e, ns = row
        assert mit_e is None,   "pasta bolognese n'a pas de données MIT"
        assert ns is None,      "nutri_score doit être NULL si mit_energy_kcal est NULL"

    def test_nutri_score_from_mit_only(self, pipeline_db):
        """
        chocolate cake : mit_energy_kcal = 350 kcal/100g → score 'D' (270–399).
        kaggle_energy_kcal = 380 kcal/portion (différente unité) — ne doit pas
        influer sur le nutri_score.
        """
        row = _query(
            pipeline_db,
            """
            SELECT mit_energy_kcal, kaggle_energy_kcal, nutri_score
            FROM recipes.recipes_main
            WHERE recipe_id = 'abc111'
            """,
        )[0]
        mit_e, kaggle_e, ns = row
        assert mit_e == pytest.approx(350.0)
        assert kaggle_e == pytest.approx(380.0)
        assert ns == "D", f"350 kcal/100g → Nutri-Score D attendu, obtenu {ns!r}"

    def test_apple_pie_nutri_score(self, pipeline_db):
        """apple pie : mit_energy_kcal = 237 kcal/100g → score 'C' (160–269)."""
        ns = _scalar(
            pipeline_db,
            "SELECT nutri_score FROM recipes.recipes_main WHERE recipe_id = 'cde333'",
        )
        assert ns == "C", f"237 kcal/100g → Nutri-Score C attendu, obtenu {ns!r}"


# ---------------------------------------------------------------------------
# 4. Table ingredients_index
# ---------------------------------------------------------------------------

class TestIngredientsIndex:

    def test_populated(self, pipeline_db):
        """L'index doit contenir au moins un ingrédient."""
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.ingredients_index")
        assert n > 0

    def test_chocolate_cake_ingredients(self, pipeline_db):
        """chocolate cake : flour + sugar + eggs (3 ingrédients validés)."""
        ingrs = {
            r[0]
            for r in _query(
                pipeline_db,
                "SELECT ingredient FROM recipes.ingredients_index WHERE recipe_id = 'abc111'",
            )
        }
        assert "flour" in ingrs
        assert "sugar" in ingrs
        assert "eggs" in ingrs

    def test_invalid_ingredient_excluded(self, pipeline_db):
        """
        'pie crust' a valid=False dans le fixture → ne doit PAS apparaître
        dans ingredients_index.
        """
        rows = _query(
            pipeline_db,
            "SELECT ingredient FROM recipes.ingredients_index WHERE ingredient = 'pie crust'",
        )
        assert len(rows) == 0, "'pie crust' (valid=False) ne doit pas être indexé"

    def test_filter_and_pattern(self, pipeline_db):
        """
        Filtre ET : recettes contenant 'flour' ET 'sugar'.
        Seul chocolate cake doit matcher.
        """
        rows = _query(
            pipeline_db,
            """
            SELECT recipe_id
            FROM recipes.ingredients_index
            WHERE ingredient IN ('flour', 'sugar')
            GROUP BY recipe_id
            HAVING COUNT(DISTINCT ingredient) = 2
            """,
        )
        recipe_ids = {r[0] for r in rows}
        assert recipe_ids == {"abc111"}


# ---------------------------------------------------------------------------
# 5. Table recipes_nutrition_detail
# ---------------------------------------------------------------------------

class TestNutritionDetail:

    def test_only_recipes_with_mit_data(self, pipeline_db):
        """
        Seules chocolate cake et apple pie ont des données MIT →
        2 lignes dans recipes_nutrition_detail.
        pasta bolognese (pas de MIT) ne doit pas apparaître.
        """
        n = _scalar(pipeline_db, "SELECT COUNT(*) FROM recipes.recipes_nutrition_detail")
        assert n == 2

    def test_pasta_not_in_nutrition_detail(self, pipeline_db):
        rows = _query(
            pipeline_db,
            "SELECT * FROM recipes.recipes_nutrition_detail WHERE recipe_id = 'bcd222'",
        )
        assert len(rows) == 0, "pasta bolognese ne doit pas avoir de ligne nutrition"
