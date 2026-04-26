-- =============================================================================
-- 02_final_tables.sql
-- Création des 3 tables finales depuis v_assembled.
-- Applique les enrichissements calculés (nutri_score, cook_time_category)
-- via les macros DuckDB — reproduit les UDFs Spark sans PySpark.
-- =============================================================================

-- Nutri-Score simplifié
CREATE OR REPLACE MACRO nutri_score(kcal) AS
    CASE
        WHEN kcal IS NULL THEN NULL
        WHEN kcal < 80    THEN 'A'
        WHEN kcal < 160   THEN 'B'
        WHEN kcal < 270   THEN 'C'
        WHEN kcal < 400   THEN 'D'
        ELSE                   'E'
    END;

-- Catégorie de temps de cuisson
CREATE OR REPLACE MACRO cook_time_cat(minutes) AS
    CASE
        WHEN minutes IS NULL THEN 'inconnu'
        WHEN minutes <= 30   THEN 'rapide'
        WHEN minutes <= 60   THEN 'moyen'
        ELSE                      'long'
    END;

-- =============================================================================
-- Table 1 : recipes_main
-- =============================================================================
CREATE OR REPLACE TABLE recipes.recipes_main AS
SELECT
    recipe_id,
    title,
    description,
    instructions_text,
    ingredients_raw,
    ingredients_validated,
    n_ingredients_validated,
    n_steps,
    cook_minutes,
    cook_time_cat(cook_minutes)     AS cook_time_category,
    image_url,
    image_urls,
    has_image,
    source_url,
    energy_kcal,
    nutri_score(energy_kcal)        AS nutri_score,
    COALESCE(tags, [])              AS tags
FROM recipes.v_assembled
QUALIFY ROW_NUMBER() OVER (PARTITION BY recipe_id ORDER BY recipe_id) = 1
;

CREATE UNIQUE INDEX IF NOT EXISTS idx_main_recipe_id
    ON recipes.recipes_main (recipe_id);
CREATE INDEX IF NOT EXISTS idx_main_nutri
    ON recipes.recipes_main (nutri_score);
CREATE INDEX IF NOT EXISTS idx_main_cook
    ON recipes.recipes_main (cook_time_category);

-- =============================================================================
-- Table 2 : ingredients_index
-- Table dénormalisée (1 ligne = 1 ingrédient × 1 recette)
-- Optimisée pour la recherche par ingrédient sans jointure
-- =============================================================================
CREATE OR REPLACE TABLE recipes.ingredients_index AS
SELECT
    recipe_id,
    title,
    nutri_score(energy_kcal)        AS nutri_score,
    image_url,
    cook_time_cat(cook_minutes)     AS cook_time_category,
    -- UNNEST equivalent du F.explode_outer Spark
    lower(trim(ingr.ingredient))    AS ingredient
FROM recipes.v_assembled
CROSS JOIN UNNEST(ingredients_validated) AS ingr(ingredient)
WHERE ingr.ingredient IS NOT NULL
  AND ingr.ingredient != ''
;

CREATE INDEX IF NOT EXISTS idx_ingr_ingredient
    ON recipes.ingredients_index (ingredient);
CREATE INDEX IF NOT EXISTS idx_ingr_recipe_id
    ON recipes.ingredients_index (recipe_id);

-- =============================================================================
-- Table 3 : recipes_nutrition_detail
-- Isolée de recipes_main pour éviter de la charger inutilement
-- =============================================================================
CREATE OR REPLACE TABLE recipes.recipes_nutrition_detail AS
SELECT
    recipe_id,
    fat_g,
    protein_g,
    salt_g,
    saturates_g,
    sugars_g
FROM recipes.v_assembled
WHERE fat_g       IS NOT NULL
   OR protein_g   IS NOT NULL
   OR salt_g      IS NOT NULL
   OR saturates_g IS NOT NULL
   OR sugars_g    IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY recipe_id ORDER BY recipe_id) = 1
;

CREATE UNIQUE INDEX IF NOT EXISTS idx_nutri_recipe_id
    ON recipes.recipes_nutrition_detail (recipe_id);

-- =============================================================================
-- Vue analytique finale (pour le dashboard / API)
-- =============================================================================
CREATE OR REPLACE VIEW recipes.v_full AS
SELECT m.*, n.fat_g, n.protein_g, n.salt_g, n.sugars_g, n.saturates_g
FROM   recipes.recipes_main       m
LEFT JOIN recipes.recipes_nutrition_detail n ON m.recipe_id = n.recipe_id
;

-- =============================================================================
-- Full-Text Search (extension DuckDB FTS)
-- =============================================================================
INSTALL fts; LOAD fts;

PRAGMA create_fts_index(
    'recipes.recipes_main',
    'recipe_id',
    'title',
    'ingredients_validated',
    stemmer      = 'french',
    stopwords    = 'none',
    lower        = 1,
    strip_accents = 1,
    overwrite    = 1
);

-- =============================================================================
-- Stats finales
-- =============================================================================
SELECT
    (SELECT COUNT(*) FROM recipes.recipes_main)            AS recipes_total,
    (SELECT COUNT(*) FROM recipes.ingredients_index)       AS index_rows,
    (SELECT COUNT(*) FROM recipes.recipes_nutrition_detail) AS nutrition_rows,
    (SELECT COUNT(*) FROM recipes.recipes_main WHERE nutri_score IS NOT NULL) AS with_nutri_score,
    (SELECT COUNT(*) FROM recipes.recipes_main WHERE has_image)               AS with_image
;
