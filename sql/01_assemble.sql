-- =============================================================================
-- 01_assemble.sql
-- Jointures en cascade entre les tables staging chargées par dlt.
-- Exécuté par DuckDB après le run dlt complet.
--
-- Convention de nommage dlt :
--   dlt crée les tables dans le dataset "recipes" (config.toml)
--   → recipes.raw_layer1, recipes.raw_layer2, recipes.raw_det_ingrs,
--     recipes.raw_nutrition, recipes.raw_kaggle
-- =============================================================================

-- Normalisation du titre pour la clé de jointure MIT ↔ Kaggle
-- (même logique que _normalize_title dans kaggle_recipes.py)
CREATE OR REPLACE MACRO normalize_title(t) AS
    lower(trim(regexp_replace(t, '[^a-zA-Z0-9\s]', '', 'g')));

-- =============================================================================
-- Vue intermédiaire : assemblage complet (LEFT JOINs en cascade)
-- Reproduit la Phase 2 du pipeline Spark original.
-- =============================================================================
CREATE OR REPLACE VIEW recipes.v_assembled AS
SELECT
    -- Identité (source MIT layer1)
    l1.id                                                       AS recipe_id,
    l1.title,
    normalize_title(l1.title)                                   AS title_norm,
    l1.url                                                      AS source_url,
    l1.instructions_text,
    l1.ingredients_raw,
    l1.n_steps,

    -- Ingrédients validés (det_ingrs)
    COALESCE(di.ingredients_validated, [])                      AS ingredients_validated,
    COALESCE(di.n_ingredients_validated, 0)                     AS n_ingredients_validated,

    -- Images (layer2)
    l2.image_url,
    COALESCE(l2.image_urls, [])                                 AS image_urls,
    COALESCE(l2.has_image, false)                               AS has_image,

    -- Métadonnées Food.com / Kaggle
    kg.description,
    kg.minutes                                                  AS cook_minutes,
    kg.tags,
    kg.kaggle_energy_kcal,

    -- Nutrition MIT (prioritaire)
    nut.energy                                                  AS mit_energy_kcal,
    nut.fat                                                     AS fat_g,
    nut.protein                                                 AS protein_g,
    nut.salt                                                    AS salt_g,
    nut.saturates                                               AS saturates_g,
    nut.sugars                                                  AS sugars_g,

    -- Énergie réconciliée (COALESCE MIT → Kaggle)
    COALESCE(nut.energy, kg.kaggle_energy_kcal)                 AS energy_kcal

FROM       recipes.raw_layer1       l1
LEFT JOIN  recipes.raw_layer2       l2   ON l1.id    = l2.id
LEFT JOIN  recipes.raw_det_ingrs    di   ON l1.id    = di.id
LEFT JOIN  recipes.raw_nutrition    nut  ON normalize_title(l1.title) = normalize_title(nut.title)
LEFT JOIN  recipes.raw_kaggle       kg   ON normalize_title(l1.title) = kg.title_norm
;

-- =============================================================================
-- Sanity check rapide après assemblage
-- =============================================================================
SELECT
    COUNT(*)                                                    AS total_recipes,
    COUNT(image_url)                                            AS with_image,
    COUNT(energy_kcal)                                          AS with_energy,
    COUNT(description)                                          AS with_description,
    ROUND(100.0 * COUNT(image_url)    / COUNT(*), 1)            AS pct_image,
    ROUND(100.0 * COUNT(energy_kcal)  / COUNT(*), 1)            AS pct_energy
FROM recipes.v_assembled;
