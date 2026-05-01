"""Tests unitaires — fonctions utilitaires et générateurs dlt des sources."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from le_grand_livre_des_recettes.pipeline.sources._utils import normalize_title
from le_grand_livre_des_recettes.pipeline.sources.kaggle_recipes import (
    _parse_list_str,
    _parse_nutrition,
)
from le_grand_livre_des_recettes.pipeline.sources.mit_recipes import (
    det_ingrs as det_ingrs_resource,
    layer1 as layer1_resource,
    layer2 as layer2_resource,
    nutrition as nutrition_resource,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
_CFG = "le_grand_livre_des_recettes.pipeline.config"


class TestNormalizeTitle:
    """La normalisation doit produire des clés identiques entre MIT et Kaggle."""

    def test_lowercase(self) -> None:
        assert normalize_title("Chicken Tikka Masala") == "chicken tikka masala"

    def test_removes_punctuation(self) -> None:
        assert normalize_title("Chicken & Rice!") == "chicken  rice"

    def test_trims_whitespace(self) -> None:
        assert normalize_title("  Pasta  ") == "pasta"

    def test_numbers_preserved(self) -> None:
        assert normalize_title("3 Cheese Pizza") == "3 cheese pizza"

    def test_empty_string(self) -> None:
        assert normalize_title("") == ""

    def test_none_safe(self) -> None:
        assert normalize_title(None) == ""  # type: ignore[arg-type]


class TestParseListStr:
    def test_standard(self) -> None:
        assert _parse_list_str("['italian', 'pasta', 'quick']") == ["italian", "pasta", "quick"]

    def test_empty_list(self) -> None:
        assert _parse_list_str("[]") == []

    def test_empty_string(self) -> None:
        assert _parse_list_str("") == []

    def test_single_item(self) -> None:
        assert _parse_list_str("['vegan']") == ["vegan"]


class TestParseNutrition:
    """_parse_nutrition extrait uniquement la première valeur (kcal/portion Kaggle)."""

    def test_extracts_first_value(self) -> None:
        assert _parse_nutrition("[312.4, 12.0, 5.2]") == pytest.approx(312.4)

    def test_empty_returns_none(self) -> None:
        assert _parse_nutrition("") is None

    def test_invalid_returns_none(self) -> None:
        assert _parse_nutrition("[bad]") is None


class TestLayer1Resource:
    def test_record_count(self) -> None:
        with patch(f"{_CFG}.LAYER1_PATH", FIXTURES_DIR / "layer1.json"):
            records = list(layer1_resource())
        assert len(records) == 3

    def test_required_fields(self) -> None:
        with patch(f"{_CFG}.LAYER1_PATH", FIXTURES_DIR / "layer1.json"):
            records = list(layer1_resource())
        required = {"id", "title", "title_norm", "instructions_text", "ingredients_raw", "n_steps"}
        for rec in records:
            assert required.issubset(rec.keys())

    def test_instructions_concatenated(self) -> None:
        with patch(f"{_CFG}.LAYER1_PATH", FIXTURES_DIR / "layer1.json"):
            choc = next(r for r in layer1_resource() if r["id"] == "abc111")
        assert "Preheat oven to 350F." in choc["instructions_text"]
        assert " | " in choc["instructions_text"]

    def test_title_norm_applied(self) -> None:
        with patch(f"{_CFG}.LAYER1_PATH", FIXTURES_DIR / "layer1.json"):
            choc = next(r for r in layer1_resource() if r["id"] == "abc111")
        assert choc["title_norm"] == "chocolate cake"


class TestLayer2Resource:
    def test_first_image_extracted(self) -> None:
        with patch(f"{_CFG}.LAYER2_PATH", FIXTURES_DIR / "layer2+.json"):
            choc = next(r for r in layer2_resource() if r["id"] == "abc111")
        assert choc["image_url"] == "http://img.example.com/choc-cake-1.jpg"
        assert len(choc["image_urls"]) == 2
        assert choc["has_image"] is True

    def test_only_recipes_with_images(self) -> None:
        with patch(f"{_CFG}.LAYER2_PATH", FIXTURES_DIR / "layer2+.json"):
            records = list(layer2_resource())
        assert len(records) == 2


class TestDetIngrsResource:
    def test_filters_invalid_ingredients(self) -> None:
        """tomatoes a valid=False → ne doit pas apparaître dans ingredients_validated."""
        with patch(f"{_CFG}.DET_INGRS_PATH", FIXTURES_DIR / "det_ingrs.json"):
            pasta = next(r for r in det_ingrs_resource() if r["id"] == "bcd222")
        assert "tomatoes" not in pasta["ingredients_validated"]
        assert "pasta" in pasta["ingredients_validated"]
        assert pasta["n_ingredients_validated"] == 2


class TestNutritionResource:
    def test_flattens_nutr_values(self) -> None:
        with patch(f"{_CFG}.NUTR_PATH", FIXTURES_DIR / "recipes_with_nutritional_info.json"):
            choc = next(r for r in nutrition_resource() if r["title"] == "chocolate cake")
        assert choc["energy_kcal"] == pytest.approx(350.0)
        assert choc["fat_g"] == pytest.approx(15.0)
        assert choc["title_norm"] == "chocolate cake"