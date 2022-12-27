WITH fact_target_salesperson__source AS (
  SELECT * 
  FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_target_salesperson__rename_column AS (
  SELECT
    CAST(year_month AS DATE) AS year_month
    , CAST(salesperson_person_id AS INTEGER) AS salesperson_person_key
    , CAST(target_revenue AS NUMERIC) AS target_gross_amount
  FROM fact_target_salesperson__source   
)

SELECT
  year_month
  , salesperson_person_key
  , target_gross_amount
FROM fact_target_salesperson__rename_column
