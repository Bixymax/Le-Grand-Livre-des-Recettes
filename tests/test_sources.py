"""
Tests unitaires pour les fonctions de normalisation des sources.
"""

import pytest
from src.le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import (
    _normalize_title,
    _parse_python_list,
    _parse_nutrition,
)
from src.le_grand_livre_des_recettes.pipeline.sources.mit_recipes import _safe_float


class TestNormalizeTitle:
    """Vérifie que la clé de jointure est identique entre MIT et Kaggle."""

    def test_lowercase(self):
        assert _normalize_title("Chicken Tikka Masala") == "chicken tikka masala"

    def test_removes_punctuation(self):
        assert _normalize_title("Chicken Tikka Masala!") == "chicken tikka masala"

    def test_removes_apostrophe(self):
        assert _normalize_title("Ma mère's Cake") == "ma mres cake"

    def test_trims_whitespace(self):
        assert _normalize_title("  Pasta  ") == "pasta"

    def test_empty_string(self):
        assert _normalize_title("") == ""

    def test_numbers_preserved(self):
        assert _normalize_title("3 Cheese Pizza") == "3 cheese pizza"


class TestParsePythonList:
    def test_standard_tags(self):
        raw = "['italian', 'pasta', 'quick']"
        assert _parse_python_list(raw) == ["italian", "pasta", "quick"]

    def test_empty_list(self):
        assert _parse_python_list("[]") == []

    def test_empty_string(self):
        assert _parse_python_list("") == []

    def test_single_item(self):
        assert _parse_python_list("['vegan']") == ["vegan"]


class TestParseNutrition:
    def test_extracts_kcal_first(self):
        raw = "[312.4, 12.0, 5.2, 1.1, 25.0]"
        result = _parse_nutrition(raw)
        assert result[0] == pytest.approx(312.4)

    def test_empty_returns_empty(self):
        assert _parse_nutrition("[]") == []

    def test_invalid_values_skipped(self):
        raw = "[100.0, bad, 5.0]"
        result = _parse_nutrition(raw)
        assert result == [100.0, 5.0]


class TestSafeFloat:
    def test_valid_float(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_invalid_string(self):
        assert _safe_float("N/A") is None
