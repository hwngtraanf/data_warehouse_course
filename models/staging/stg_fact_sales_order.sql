WITH stg_fact_sales_orders__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_orders__rename_columns AS (
  SELECT
    order_id AS sales_order_key
    , customer_id AS customer_key
  FROM stg_fact_sales_orders__source
)

, stg_fact_sales_orders__cast_type AS (
  SELECT
    CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(customer_key AS INTEGER) AS customer_key
  FROM stg_fact_sales_orders__rename_columns
)

SELECT
  sales_order_key
  , customer_key
FROM stg_fact_sales_orders__cast_type
