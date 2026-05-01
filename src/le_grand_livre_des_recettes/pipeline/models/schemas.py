"""Schémas Pydantic documentant les contrats de données du pipeline."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class Layer1Raw(BaseModel):
    """Modèle des données d'entrée issues de layer1.json."""
    id: str
    title: str
    url: Optional[str] = None
    partition: Optional[str] = None
    ingredients: list[dict[str, Any]] = Field(default_factory=list)
    instructions: list[dict[str, Any]] = Field(default_factory=list)


class Layer2Raw(BaseModel):
    """Modèle des données d'entrée issues de layer2+.json."""
    id: str
    images: list[dict[str, Any]] = Field(default_factory=list)


class DetIngrsRaw(BaseModel):
    """Modèle des ingrédients validés issus de det_ingrs.json."""
    id: str
    ingredients: list[dict[str, Any]] = Field(default_factory=list)
    valid: list[bool] = Field(default_factory=list)


class NutritionRaw(BaseModel):
    """Modèle nutritionnel issu de recipes_with_nutritional_info.json."""
    title: str
    nutr_values_per100g: dict[str, Any] = Field(default_factory=dict)


class KaggleRaw(BaseModel):
    """Modèle des données d'entrée issues de RAW_recipes.csv."""
    name: str
    minutes: Optional[int] = None
    tags: Optional[str] = None
    nutrition: Optional[str] = None
    n_steps: Optional[int] = None
    description: Optional[str] = None
    n_ingredients: Optional[int] = None


class RecipeMain(BaseModel):
    """Contrat de la table recipes_main (partitionnement physique par nutri_score)."""
    recipe_id: str
    title: str
    description: Optional[str] = None
    instructions_text: Optional[str] = None
    ingredients_raw: list[str] = Field(default_factory=list)
    ingredients_validated: list[str] = Field(default_factory=list)
    n_ingredients_validated: int = 0
    n_steps: int = 0
    cook_minutes: Optional[int] = None
    cook_time_category: str = "inconnu"
    image_url: Optional[str] = None
    image_urls: list[str] = Field(default_factory=list)
    has_image: bool = False
    source_url: Optional[str] = None
    mit_energy_kcal: Optional[float] = None
    kaggle_energy_kcal: Optional[float] = None
    nutri_score: Optional[str] = None
    tags: list[str] = Field(default_factory=list)


class IngredientIndex(BaseModel):
    """Contrat de la table ingredients_index."""
    recipe_id: str
    title: str
    nutri_score: Optional[str] = None
    image_url: Optional[str] = None
    cook_time_category: str = "inconnu"
    ingredient: str


class RecipeNutritionDetail(BaseModel):
    """Contrat de la table recipes_nutrition_detail (valeurs exprimées en g/100g)."""
    recipe_id: str
    fat_g: Optional[float] = None
    protein_g: Optional[float] = None
    salt_g: Optional[float] = None
    saturates_g: Optional[float] = None
    sugars_g: Optional[float] = None