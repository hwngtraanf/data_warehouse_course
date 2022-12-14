WITH stg_fact_sales_orders__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_orders__rename_columns AS (
  SELECT
    order_id AS sales_order_key
    , customer_id AS customer_key
    , salesperson_person_id AS salesperson_person_key
    , picked_by_person_id AS picked_by_person_key
    , contact_person_id AS contact_person_key
    , backorder_order_id AS backorder_order_key
    , order_date
    , expected_delivery_date
    , customer_purchase_order_number
    , is_undersupply_backordered
    , comments
    , delivery_instructions
    , internal_comments
    , picking_completed_when
  FROM stg_fact_sales_orders__source
)

, stg_fact_sales_orders__cast_type AS (
  SELECT
    CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(customer_key AS INTEGER) AS customer_key
    , CAST(salesperson_person_key AS INTEGER) AS salesperson_person_key
    , CAST(picked_by_person_key AS INTEGER) AS picked_by_person_key
    , CAST(contact_person_key AS INTEGER) AS contact_person_key
    , CAST(backorder_order_key AS INTEGER) AS backorder_order_key
    , CAST(order_date AS DATE) AS order_date
    , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
    , CAST(customer_purchase_order_number AS INTEGER) AS customer_purchase_order_number
    , CAST(is_undersupply_backordered AS BOOLEAN) AS is_undersupply_backordered_boolean
    , CAST(comments AS STRING) AS comments
    , CAST(delivery_instructions AS STRING) AS delivery_instructions
    , CAST(internal_comments AS STRING) AS internal_comments
    , CAST(picking_completed_when AS DATE) AS picking_completed_when
  FROM stg_fact_sales_orders__rename_columns
)

SELECT
  sales_order_key
  , customer_key
  , COALESCE(salesperson_person_key, 0) AS salesperson_person_key
  , COALESCE(picked_by_person_key, 0) AS picked_by_person_key
  , COALESCE(contact_person_key, 0) AS contact_person_key
  , backorder_order_key
  , order_date
  , expected_delivery_date
  , customer_purchase_order_number
  -- , CASE 
  --         WHEN is_undersupply_backordered_boolean is TRUE
  --           THEN 'Undersupply Backordered'
  --         WHEN is_undersupply_backordered_boolean is FALSE 
  --           THEN 'Not Undersupply Backordered' 
  --         ELSE 'Undefined' END
  --     AS is_undersupply_backordered
  , is_undersupply_backordered_boolean AS is_undersupply_backordered
  , COALESCE(comments, 'Undefined') AS comments
  , COALESCE(delivery_instructions, 'Undefined') AS delivery_instructions
  , COALESCE(internal_comments, 'Undefined') AS internal_comments
  , picking_completed_when
FROM stg_fact_sales_orders__cast_type
