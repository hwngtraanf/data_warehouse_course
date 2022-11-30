WITH fact_sales_order_line__source AS (
  SELECT * 
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS (
  SELECT 
    order_id AS sales_order_key
    , order_line_id AS sales_order_line_key
    , stock_item_id AS product_key
    , quantity 
    , unit_price
  FROM 
    fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
  SELECT 
    CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
    , CAST(product_key AS INTEGER) AS product_key
    , CAST(quantity AS INTEGER) AS quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
  FROM 
    fact_sales_order_line__rename_column
)

, fact_sales_order_line__calculate_measure AS (
  SELECT
    *
    , quantity * unit_price AS gross_amount
  FROM 
    fact_sales_order_line__cast_type
)

, fact_sales_order_line__join_stg_fact_sales_order AS (
  SELECT
    fact_line.*
    , fact_header.customer_key
    , fact_header.picked_by_person_key
  FROM fact_sales_order_line__calculate_measure fact_line
  LEFT JOIN {{ ref('stg_fact_sales_order') }} fact_header
  ON fact_line.sales_order_key = fact_header.sales_order_key
)

SELECT 
  sales_order_line_key
  , sales_order_key
  , customer_key
  , product_key
  , picked_by_person_key
  , quantity
  , unit_price
  , gross_amount
FROM fact_sales_order_line__join_stg_fact_sales_order
