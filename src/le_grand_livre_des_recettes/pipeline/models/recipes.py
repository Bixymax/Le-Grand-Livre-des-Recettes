"""
Schémas Pydantic du pipeline.

Deux familles de modèles :

- Raw* : décrivent le format brut des fichiers sources (documentation /
  référence). Non utilisés à l'exécution.

- *Staging : décrivent exactement ce que chaque @dlt.resource yield.
  Ces modèles sont passés en `columns=` aux resources → dlt les utilise
  pour inférer le schéma des tables staging sans avoir à lire les données.
  Les instances sont directement yieldées ; dlt appelle .model_dump()
  en interne (Pydantic v2).

- Recipe* : schémas des tables finales (documentation des outputs SQL).
"""

from typing import Optional
from pydantic import BaseModel, Field


# =============================================================================
# Raw input models — format des fichiers sources (documentation uniquement)
# =============================================================================

class Layer1Raw(BaseModel):
    """Enregistrement brut issu de layer1.json (MIT Recipe1M+)."""
    id: str
    title: str
    url: Optional[str] = None
    partition: Optional[str] = None
    ingredients: list[dict] = Field(default_factory=list)
    instructions: list[dict] = Field(default_factory=list)


class Layer2Raw(BaseModel):
    """Enregistrement brut issu de layer2+.json."""
    id: str
    images: list[dict] = Field(default_factory=list)


class DetIngrsRaw(BaseModel):
    """Ingrédients validés depuis det_ingrs.json."""
    id: str
    ingredients: list[dict] = Field(default_factory=list)
    valid: list[bool] = Field(default_factory=list)


class KaggleRaw(BaseModel):
    """Enregistrement brut depuis RAW_recipes.csv (Food.com / Kaggle)."""
    name: str
    minutes: Optional[int] = None
    tags: Optional[str] = None
    nutrition: Optional[str] = None
    n_steps: Optional[int] = None
    description: Optional[str] = None
    n_ingredients: Optional[int] = None


# =============================================================================
# Staging models — contrat exact de ce que chaque @dlt.resource yield.
# Passés en `columns=` : dlt lit le schéma Pydantic pour définir les tables.
# =============================================================================

class Layer1Staging(BaseModel):
    """
    Record yielded par la resource `layer1`.
    → Table staging : recipes.raw_layer1
    """
    id: str
    title: str
    url: Optional[str] = None
    partition: Optional[str] = None
    # list[str] → child table raw_layer1__ingredients_raw (géré par dlt)
    ingredients_raw: list[str] = Field(default_factory=list)
    n_steps: int = 0
    instructions_text: str = ""


class Layer2Staging(BaseModel):
    """
    Record yielded par la resource `layer2`.
    → Table staging : recipes.raw_layer2
    """
    id: str
    # list[str] → child table raw_layer2__image_urls (géré par dlt)
    image_urls: list[str] = Field(default_factory=list)
    image_url: Optional[str] = None
    has_image: bool = False


class DetIngrStaging(BaseModel):
    """
    Record yielded par la resource `det_ingrs`.
    → Table staging : recipes.raw_det_ingrs
    """
    id: str
    # list[str] → child table raw_det_ingrs__ingredients_validated (géré par dlt)
    ingredients_validated: list[str] = Field(default_factory=list)
    n_ingredients_validated: int = 0


class NutritionStaging(BaseModel):
    """
    Record yielded par la resource `nutrition`.
    → Table staging : recipes.raw_nutrition
    Unités : kcal/100g (standard Nutri-Score européen).
    """
    title: str
    energy: Optional[float] = None      # kcal/100g
    fat: Optional[float] = None         # g/100g
    protein: Optional[float] = None     # g/100g
    salt: Optional[float] = None        # g/100g
    saturates: Optional[float] = None   # g/100g
    sugars: Optional[float] = None      # g/100g


class KaggleStaging(BaseModel):
    """
    Record yielded par la resource `kaggle_raw`.
    → Table staging : recipes.raw_kaggle

    ATTENTION — unité énergie :
      `kaggle_energy_kcal` est en kcal/PORTION (pas /100g).
      Elle NE DOIT PAS être utilisée pour le Nutri-Score.
      Utiliser NutritionStaging.energy (MIT, kcal/100g) à la place.
    """
    name: str
    title_norm: str                             # clé de jointure normalisée
    minutes: Optional[int] = None
    n_steps: Optional[int] = None
    n_ingredients: Optional[int] = None
    description: Optional[str] = None
    # list[str] → child table raw_kaggle__tags (géré par dlt)
    tags: list[str] = Field(default_factory=list)
    kaggle_energy_kcal: Optional[float] = None  # kcal/portion ≠ kcal/100g


# =============================================================================
# Output schemas — tables finales (documentation uniquement)
# =============================================================================

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
    # ---- Énergie — unités séparées, ne pas mélanger ----
    mit_energy_kcal: Optional[float] = None     # kcal/100g  → Nutri-Score
    kaggle_energy_kcal: Optional[float] = None  # kcal/portion → affichage
    nutri_score: Optional[str] = None           # calculé sur mit_energy_kcal uniquement
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
