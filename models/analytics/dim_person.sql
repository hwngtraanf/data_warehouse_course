WITH dim_person__source AS (
  SELECT
    *
  FROM
    `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_columns AS (
  SELECT
    person_id AS person_key
    , full_name
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT 
    CAST(person_key AS INTEGER) AS person_key
    , CAST(full_name AS STRING) AS full_name
  FROM dim_person__rename_columns
)

SELECT 
  person_key
  , full_name
FROM dim_person__cast_type
