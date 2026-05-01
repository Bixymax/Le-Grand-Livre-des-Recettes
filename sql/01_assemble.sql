-- =============================================================================
-- 01_assemble.sql — Référence DuckDB (ancienne implémentation)
-- Ce fichier est conservé à titre documentaire uniquement.
-- La logique équivalente est implémentée en PySpark dans :
--   pipeline/transformers/assemble.py
-- =============================================================================

CREATE OR REPLACE MACRO normalize_title(t) AS
    lower(trim(regexp_replace(t, '[^a-zA-Z0-9\s]', '', 'g')));

COPY (
    SELECT *, normalize_title(title) AS title_norm
    FROM recipes.raw_layer1
) TO 'data/outputs/tmp/tmp_l1.parquet' (FORMAT PARQUET);

COPY (
    SELECT _dlt_parent_id, list(value) AS ingredients_raw
    FROM (SELECT _dlt_parent_id, value FROM recipes.raw_layer1__ingredients_raw ORDER BY _dlt_parent_id, _dlt_list_idx)
    GROUP BY _dlt_parent_id
) TO 'data/outputs/tmp/tmp_ingrs.parquet' (FORMAT PARQUET);

COPY (
    SELECT _dlt_parent_id, list(value) AS image_urls
    FROM (SELECT _dlt_parent_id, value FROM recipes.raw_layer2__image_urls ORDER BY _dlt_parent_id, _dlt_list_idx)
    GROUP BY _dlt_parent_id
) TO 'data/outputs/tmp/tmp_imgs.parquet' (FORMAT PARQUET);

COPY (
    SELECT _dlt_parent_id, list(value) AS tags
    FROM (SELECT _dlt_parent_id, value FROM recipes.raw_kaggle__tags ORDER BY _dlt_parent_id, _dlt_list_idx)
    GROUP BY _dlt_parent_id
) TO 'data/outputs/tmp/tmp_tags.parquet' (FORMAT PARQUET);

COPY (
    SELECT _dlt_parent_id, list(value) AS ingredients_validated
    FROM (SELECT _dlt_parent_id, value FROM recipes.raw_det_ingrs__ingredients_validated ORDER BY _dlt_parent_id, _dlt_list_idx)
    GROUP BY _dlt_parent_id
) TO 'data/outputs/tmp/tmp_det_ingrs.parquet' (FORMAT PARQUET);

DROP TABLE IF EXISTS recipes.v_assembled;

CREATE TABLE recipes.v_assembled AS
WITH RawAssembled AS (
    SELECT
        l1.id                                                       AS recipe_id,
        l1.title,
        l1.title_norm,
        l1.url                                                      AS source_url,
        l1.instructions_text,
        COALESCE(ing.ingredients_raw,  [])                          AS ingredients_raw,
        l1.n_steps,
        COALESCE(dingrs.ingredients_validated, [])                  AS ingredients_validated,
        COALESCE(di.n_ingredients_validated, 0)                     AS n_ingredients_validated,
        l2.image_url,
        COALESCE(img.image_urls, [])                                AS image_urls,
        COALESCE(l2.has_image, false)                               AS has_image,
        kg.description,
        kg.minutes                                                  AS cook_minutes,
        COALESCE(tgs.tags, [])                                      AS tags,
        kg.kaggle_energy_kcal,
        nut.energy                                                  AS mit_energy_kcal,
        nut.fat                                                     AS fat_g,
        nut.protein                                                 AS protein_g,
        nut.salt                                                    AS salt_g,
        nut.saturates                                               AS saturates_g,
        nut.sugars                                                  AS sugars_g

    FROM       read_parquet('data/outputs/tmp/tmp_l1.parquet')        l1
    LEFT JOIN  recipes.raw_layer2                                     l2      ON l1.id         = l2.id
    LEFT JOIN  recipes.raw_det_ingrs                                  di      ON l1.id         = di.id
    LEFT JOIN  recipes.raw_nutrition                                  nut     ON l1.title_norm = normalize_title(nut.title)
    LEFT JOIN  recipes.raw_kaggle                                     kg      ON l1.title_norm = kg.title_norm
    LEFT JOIN  read_parquet('data/outputs/tmp/tmp_ingrs.parquet')     ing     ON l1._dlt_id    = ing._dlt_parent_id
    LEFT JOIN  read_parquet('data/outputs/tmp/tmp_imgs.parquet')      img     ON l2._dlt_id    = img._dlt_parent_id
    LEFT JOIN  read_parquet('data/outputs/tmp/tmp_tags.parquet')      tgs     ON kg._dlt_id    = tgs._dlt_parent_id
    LEFT JOIN  read_parquet('data/outputs/tmp/tmp_det_ingrs.parquet') dingrs  ON di._dlt_id    = dingrs._dlt_parent_id
)
SELECT *
FROM RawAssembled
QUALIFY ROW_NUMBER() OVER (PARTITION BY recipe_id ORDER BY recipe_id) = 1;

SELECT
    COUNT(*)                                                       AS total_recipes,
    ROUND(100.0 * COUNT(image_url)       / COUNT(*), 1)            AS pct_image,
    ROUND(100.0 * COUNT(mit_energy_kcal) / COUNT(*), 1)            AS pct_mit_energy
FROM recipes.v_assembled;
