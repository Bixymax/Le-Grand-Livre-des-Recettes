-- =============================================================================
-- 01_assemble.sql — v5
-- Jointures en cascade entre les tables staging chargées par dlt.
--
-- Comportement dlt (yield instances Pydantic, sans columns=) :
--   Les champs list[str] sont normalisés par dlt en child tables :
--     raw_layer1__ingredients_raw       (value, _dlt_parent_id, _dlt_list_idx)
--     raw_layer2__image_urls            (value, _dlt_parent_id, _dlt_list_idx)
--     raw_det_ingrs__ingredients_validated (value, _dlt_parent_id, _dlt_list_idx)
--     raw_kaggle__tags                  (value, _dlt_parent_id, _dlt_list_idx)
--   Les CTEs ci-dessous reconstruisent ces listes en VARCHAR[] DuckDB natifs
--   via list(value ORDER BY _dlt_list_idx).
-- =============================================================================

-- Normalisation du titre pour la clé de jointure MIT ↔ Kaggle
CREATE OR REPLACE MACRO normalize_title(t) AS
    lower(trim(regexp_replace(t, '[^a-zA-Z0-9\s]', '', 'g')));

-- =============================================================================
-- Vue intermédiaire : assemblage complet
-- =============================================================================
CREATE OR REPLACE VIEW recipes.v_assembled AS
WITH
-- OPTIM : title_norm pré-calculé une seule fois (évite N appels macro dans les JOINs)
CTE_L1 AS (
    SELECT
        *,
        normalize_title(title) AS title_norm
    FROM recipes.raw_layer1
),

-- Ingrédients bruts (layer1 → child table dlt)
CTE_Ingredients AS (
    SELECT
        _dlt_parent_id,
        list(value ORDER BY _dlt_list_idx) AS ingredients_raw
    FROM recipes.raw_layer1__ingredients_raw
    GROUP BY _dlt_parent_id
),

-- URLs d'images (layer2 → child table dlt)
CTE_Images AS (
    SELECT
        _dlt_parent_id,
        list(value ORDER BY _dlt_list_idx) AS image_urls
    FROM recipes.raw_layer2__image_urls
    GROUP BY _dlt_parent_id
),

-- Tags Kaggle (raw_kaggle → child table dlt)
CTE_Tags AS (
    SELECT
        _dlt_parent_id,
        list(value ORDER BY _dlt_list_idx) AS tags
    FROM recipes.raw_kaggle__tags
    GROUP BY _dlt_parent_id
),

-- Ingrédients validés (det_ingrs → child table dlt)
CTE_DetIngrs AS (
    SELECT
        _dlt_parent_id,
        list(value ORDER BY _dlt_list_idx) AS ingredients_validated
    FROM recipes.raw_det_ingrs__ingredients_validated
    GROUP BY _dlt_parent_id
)

SELECT
    -- Identité (source MIT layer1)
    l1.id                                                       AS recipe_id,
    l1.title,
    l1.title_norm,
    l1.url                                                      AS source_url,
    l1.instructions_text,
    COALESCE(ing.ingredients_raw,  [])                          AS ingredients_raw,
    l1.n_steps,

    -- Ingrédients validés reconstruits depuis la child table dlt
    COALESCE(dingrs.ingredients_validated, [])                  AS ingredients_validated,
    COALESCE(di.n_ingredients_validated, 0)                     AS n_ingredients_validated,

    -- Images (layer2)
    l2.image_url,
    COALESCE(img.image_urls, [])                                AS image_urls,
    COALESCE(l2.has_image, false)                               AS has_image,

    -- Métadonnées Food.com / Kaggle
    kg.description,
    kg.minutes                                                  AS cook_minutes,
    COALESCE(tgs.tags, [])                                      AS tags,
    kg.kaggle_energy_kcal,

    -- Nutrition MIT (kcal/100g — standard Nutri-Score, source prioritaire)
    nut.energy                                                  AS mit_energy_kcal,
    nut.fat                                                     AS fat_g,
    nut.protein                                                 AS protein_g,
    nut.salt                                                    AS salt_g,
    nut.saturates                                               AS saturates_g,
    nut.sugars                                                  AS sugars_g
    -- NOTE : kaggle_energy_kcal (kcal/portion) est exposé séparément.
    -- Les deux colonnes restent séparées — leurs unités sont incompatibles.
    -- Le Nutri-Score est calculé exclusivement sur mit_energy_kcal (par 100 g).

FROM       CTE_L1                  l1
LEFT JOIN  recipes.raw_layer2      l2      ON l1.id         = l2.id
LEFT JOIN  recipes.raw_det_ingrs   di      ON l1.id         = di.id
-- OPTIM : jointure sur title_norm pré-calculé (plus d'appel macro dans le JOIN)
LEFT JOIN  recipes.raw_nutrition   nut     ON l1.title_norm = normalize_title(nut.title)
LEFT JOIN  recipes.raw_kaggle      kg      ON l1.title_norm = kg.title_norm
-- Child tables dlt reconstruites via CTEs
LEFT JOIN  CTE_Ingredients         ing     ON l1._dlt_id    = ing._dlt_parent_id
LEFT JOIN  CTE_Images              img     ON l2._dlt_id    = img._dlt_parent_id
LEFT JOIN  CTE_Tags                tgs     ON kg._dlt_id    = tgs._dlt_parent_id
LEFT JOIN  CTE_DetIngrs            dingrs  ON di._dlt_id    = dingrs._dlt_parent_id
;

-- =============================================================================
-- Sanity check rapide après assemblage
-- =============================================================================
SELECT
    COUNT(*)                                                       AS total_recipes,
    COUNT(image_url)                                               AS with_image,
    COUNT(mit_energy_kcal)                                         AS with_mit_energy,
    COUNT(kaggle_energy_kcal)                                      AS with_kaggle_energy,
    COUNT(description)                                             AS with_description,
    COUNT(CASE WHEN array_length(ingredients_validated) > 0
               THEN 1 END)                                         AS with_validated_ingrs,
    ROUND(100.0 * COUNT(image_url)       / COUNT(*), 1)            AS pct_image,
    ROUND(100.0 * COUNT(mit_energy_kcal) / COUNT(*), 1)            AS pct_mit_energy
FROM recipes.v_assembled;
