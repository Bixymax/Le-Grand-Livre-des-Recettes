"""
Schémas Pydantic du pipeline — documentation des contrats de données.

Ces modèles ne sont pas utilisés à l'exécution du pipeline PySpark.
Ils servent de référence documentaire pour les formats de fichiers sources
et les tables finales produites, et peuvent être utilisés pour valider
des échantillons de données en tests ou dans un notebook d'exploration.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# =============================================================================
# Sources brutes — format des fichiers d'entrée
# =============================================================================

class Layer1Raw(BaseModel):
    """Enregistrement issu de layer1.json (MIT Recipe1M+)."""

    id: str
    title: str
    url: Optional[str] = None
    partition: Optional[str] = None
    ingredients: list[dict] = Field(default_factory=list)
    instructions: list[dict] = Field(default_factory=list)


class Layer2Raw(BaseModel):
    """Enregistrement issu de layer2+.json (images)."""

    id: str
    images: list[dict] = Field(default_factory=list)


class DetIngrsRaw(BaseModel):
    """Ingrédients validés depuis det_ingrs.json."""

    id: str
    ingredients: list[dict] = Field(default_factory=list)
    valid: list[bool] = Field(default_factory=list)


class NutritionRaw(BaseModel):
    """Enregistrement issu de recipes_with_nutritional_info.json."""

    title: str
    nutr_values_per100g: dict = Field(default_factory=dict)


class KaggleRaw(BaseModel):
    """Enregistrement issu de RAW_recipes.csv (Food.com / Kaggle)."""

    name: str
    minutes: Optional[int] = None
    tags: Optional[str] = None
    nutrition: Optional[str] = None
    n_steps: Optional[int] = None
    description: Optional[str] = None
    n_ingredients: Optional[int] = None


# =============================================================================
# Tables finales — contrat des outputs Parquet
# =============================================================================

class RecipeMain(BaseModel):
    """
    Table ``recipes_main`` — une ligne par recette.

    Partitionnée physiquement par ``nutri_score``.
    """

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
    mit_energy_kcal: Optional[float] = None      # kcal/100g  → Nutri-Score
    kaggle_energy_kcal: Optional[float] = None   # kcal/portion → affichage uniquement
    nutri_score: Optional[str] = None            # calculé sur mit_energy_kcal
    tags: list[str] = Field(default_factory=list)


class IngredientIndex(BaseModel):
    """
    Table ``ingredients_index`` — une ligne par (recette × ingrédient).

    Permet le filtrage rapide par ingrédient sans scanner les arrays.
    """

    recipe_id: str
    title: str
    nutri_score: Optional[str] = None
    image_url: Optional[str] = None
    cook_time_category: str = "inconnu"
    ingredient: str


class RecipeNutritionDetail(BaseModel):
    """
    Table ``recipes_nutrition_detail`` — détail nutritionnel par recette.

    Toutes les valeurs en g/100g (source MIT uniquement).
    """

    recipe_id: str
    fat_g: Optional[float] = None
    protein_g: Optional[float] = None
    salt_g: Optional[float] = None
    saturates_g: Optional[float] = None
    sugars_g: Optional[float] = None
