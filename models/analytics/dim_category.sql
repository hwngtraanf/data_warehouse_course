WITH dim_category__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.external__categories`
)

, dim_category__rename_columns AS (
  SELECT
    category_id AS category_key
    , category_name 
    , parent_category_id AS parent_category_key
    , category_level
  FROM dim_category__source
)

, dim_category__cast_type AS (
  SELECT
    CAST(category_key AS INTEGER) AS category_key
    , CAST(category_name AS STRING) AS category_name
    , CAST(parent_category_key AS INTEGER) AS parent_category_key
    , CAST(category_level AS INTEGER) AS category_level
  FROM dim_category__rename_columns
)

, dim_category__handle_null AS (
  SELECT
    category_key
    , COALESCE(category_name, 'Undefined') AS category_name
    , parent_category_key
    , category_level
  FROM dim_category__cast_type
  )

SELECT
  category_key
  , category_name
  , parent_category_key
  , category_level
FROM dim_category__handle_null