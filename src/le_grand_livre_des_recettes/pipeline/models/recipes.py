"""
Schémas Pydantic utilisés pour le typage dlt.
Ils servent de contrat de données entre les sources et les transformers.
dlt les utilise pour inférer le schéma des tables et valider les données.
"""

from typing import Optional
from pydantic import BaseModel, Field


class Layer1Record(BaseModel):
    """Enregistrement brut issu de layer1.json (MIT Recipe1M+)."""
    id: str
    title: str
    url: Optional[str] = None
    partition: Optional[str] = None
    ingredients: list[dict] = Field(default_factory=list)
    instructions: list[dict] = Field(default_factory=list)


class Layer2Record(BaseModel):
    """Enregistrement brut issu de layer2+.json — URLs des images."""
    id: str
    images: list[dict] = Field(default_factory=list)


class DetIngrsRecord(BaseModel):
    """Ingrédients validés depuis det_ingrs.json."""
    id: str
    ingredients: list[dict] = Field(default_factory=list)


class KaggleRecord(BaseModel):
    """Enregistrement brut depuis RAW_recipes.csv (Food.com / Kaggle)."""
    name: str
    minutes: Optional[int] = None
    tags: Optional[str] = None
    nutrition: Optional[str] = None
    n_steps: Optional[int] = None
    description: Optional[str] = None
    n_ingredients: Optional[int] = None


class NutritionRecord(BaseModel):
    """Nutrition depuis recipes_with_nutritional_info.json."""
    title: str
    energy: Optional[float] = None
    fat: Optional[float] = None
    protein: Optional[float] = None
    salt: Optional[float] = None
    saturates: Optional[float] = None
    sugars: Optional[float] = None


# ---- Tables finales (output schemas) ----

class RecipeMain(BaseModel):
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
    energy_kcal: Optional[float] = None
    nutri_score: Optional[str] = None
    tags: list[str] = Field(default_factory=list)


class IngredientIndex(BaseModel):
    recipe_id: str
    title: str
    nutri_score: Optional[str] = None
    image_url: Optional[str] = None
    cook_time_category: str = "inconnu"
    ingredient: str


class RecipeNutritionDetail(BaseModel):
    recipe_id: str
    fat_g: Optional[float] = None
    protein_g: Optional[float] = None
    salt_g: Optional[float] = None
    saturates_g: Optional[float] = None
    sugars_g: Optional[float] = None
