WITH dim_city__source AS (
  SELECT
    *
  FROM
    `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city__rename_columns AS (
  SELECT
    city_id AS city_key
    , city_name
    , state_province_id AS state_province_key
  FROM dim_city__source
)

, dim_city__cast_type AS (
  SELECT 
    CAST(city_key AS INTEGER) AS city_key
    , CAST(city_name AS STRING) AS city_name
    , CAST(state_province_key AS INTEGER) AS state_province_key
  FROM dim_city__rename_columns
)

SELECT 
  dim_city.city_key
  , COALESCE(dim_city.city_name, 'Undefined') AS city_name
  , dim_city.state_province_key
  , COALESCE(dim_state_province.state_province_name, 'Undefined') AS state_province_name
FROM dim_city__cast_type dim_city
LEFT JOIN {{ ref('stg_dim_state_province') }} dim_state_province
ON dim_city.state_province_key = dim_state_province.state_province_key
