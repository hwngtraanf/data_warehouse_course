WITH dim_sales_customers__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_sales_customers__rename_columns AS (
  SELECT
    customer_id AS customer_key
    , customer_name 
  FROM dim_sales_customers__source
)

, dim_sales_customers__cast_type AS (
  SELECT
    CAST(customer_key AS INTEGER) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
  FROM dim_sales_customers__rename_columns
)

SELECT
  customer_key
  , customer_name
FROM dim_sales_customers__cast_type

