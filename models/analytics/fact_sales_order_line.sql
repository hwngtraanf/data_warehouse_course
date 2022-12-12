  WITH fact_sales_order_line__source AS (
    SELECT * 
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
  )

  , fact_sales_order_line__rename_column AS (
    SELECT 
      order_id AS sales_order_key
      , description
      , order_line_id AS sales_order_line_key
      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , quantity 
      , unit_price
      , tax_rate
      , picked_quantity
      , picking_completed_when
    FROM 
      fact_sales_order_line__source
  )

  , fact_sales_order_line__cast_type AS (
    SELECT 
      CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST(description AS STRING) AS description
      , CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(quantity AS INTEGER) AS quantity
      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(picked_quantity AS INTEGER) AS picked_quantity
      , CAST(picking_completed_when AS DATE) AS picking_completed_when
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
      , COALESCE(fact_header.salesperson_person_key, -1) AS salesperson_person_key
      , COALESCE(fact_header.picked_by_person_key, -1) AS picked_by_person_key
      , COALESCE(fact_header.contact_person_key, -1) AS contact_person_key
      , fact_header.backorder_order_key
      , fact_header.order_date
      , fact_header.expected_delivery_date
      , fact_header.customer_purchase_order_number
      , fact_header.is_undersupply_backordered
      , fact_header.comments
      , fact_header.delivery_instructions
      , fact_header.internal_comments
    FROM fact_sales_order_line__calculate_measure fact_line
    LEFT JOIN {{ ref('stg_fact_sales_order') }} fact_header
    ON fact_line.sales_order_key = fact_header.sales_order_key
  )

  SELECT 
    sales_order_line_key
    , description
    , sales_order_key
    , customer_key
    , product_key
    , salesperson_person_key
    , picked_by_person_key
    , contact_person_key
    , backorder_order_key
    , package_type_key
    , order_date
    , expected_delivery_date
    , customer_purchase_order_number
    , is_undersupply_backordered
    , comments
    , delivery_instructions
    , internal_comments
    , quantity
    , unit_price
    , gross_amount
    , tax_rate
    , picked_quantity
    , picking_completed_when
  FROM fact_sales_order_line__join_stg_fact_sales_order
