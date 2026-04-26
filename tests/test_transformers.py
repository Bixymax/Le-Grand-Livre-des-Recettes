"""
Tests unitaires pour les fonctions d'enrichissement.

Ces tests ne nécessitent aucune dépendance externe (pas de DuckDB, pas de fichiers).
Ils couvrent la logique métier pure extraite dans pipeline/transformers/enrich.py.
"""

import pytest
from src.le_grand_livre_des_recettes.pipeline.transformers.enrich import (
    compute_nutri_score,
    compute_cook_time_category,
    coalesce_energy,
)


class TestNutriScore:
    def test_score_a(self):
        assert compute_nutri_score(50.0) == "A"
        assert compute_nutri_score(0.0) == "A"
        assert compute_nutri_score(79.9) == "A"

    def test_score_b(self):
        assert compute_nutri_score(80.0) == "B"
        assert compute_nutri_score(120.0) == "B"
        assert compute_nutri_score(159.9) == "B"

    def test_score_c(self):
        assert compute_nutri_score(160.0) == "C"
        assert compute_nutri_score(200.0) == "C"
        assert compute_nutri_score(269.9) == "C"

    def test_score_d(self):
        assert compute_nutri_score(270.0) == "D"
        assert compute_nutri_score(300.0) == "D"
        assert compute_nutri_score(399.9) == "D"

    def test_score_e(self):
        assert compute_nutri_score(400.0) == "E"
        assert compute_nutri_score(999.0) == "E"

    def test_none_returns_none(self):
        assert compute_nutri_score(None) is None


class TestCookTimeCategory:
    def test_rapide(self):
        assert compute_cook_time_category(0) == "rapide"
        assert compute_cook_time_category(30) == "rapide"

    def test_moyen(self):
        assert compute_cook_time_category(31) == "moyen"
        assert compute_cook_time_category(60) == "moyen"

    def test_long(self):
        assert compute_cook_time_category(61) == "long"
        assert compute_cook_time_category(240) == "long"

    def test_none_is_inconnu(self):
        assert compute_cook_time_category(None) == "inconnu"


class TestCoalesceEnergy:
    def test_mit_prioritaire(self):
        assert coalesce_energy(100.0, 200.0) == 100.0

    def test_fallback_kaggle(self):
        assert coalesce_energy(None, 200.0) == 200.0

    def test_both_none(self):
        assert coalesce_energy(None, None) is None

    def test_mit_zero_is_valid(self):
        # 0 est une valeur valide (recette très légère), pas un null
        assert coalesce_energy(0.0, 200.0) == 0.0
