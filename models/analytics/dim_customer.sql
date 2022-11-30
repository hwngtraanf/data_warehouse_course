WITH dim_customer__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_columns AS (
  SELECT
    customer_id AS customer_key
    , customer_name 
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
    , is_on_credit_hold
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT
    CAST(customer_key AS INTEGER) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(customer_category_key AS INTEGER) AS customer_category_key
    , CAST(buying_group_key AS INTEGER) AS buying_group_key
    , CAST(is_on_credit_hold AS BOOLEAN) AS is_on_credit_hold_boolean
  FROM dim_customer__rename_columns
)

, dim_customer__handle_null AS (
  SELECT
    customer_key
    , COALESCE(customer_name, 'Undefined') AS customer_name
    , customer_category_key
    , buying_group_key
    , is_on_credit_hold_boolean
  FROM dim_customer__cast_type
  )

, dim_customer__convert_boolean AS (
  SELECT
    customer_key
    , customer_name
    , customer_category_key
    , buying_group_key
    , CASE 
          WHEN is_on_credit_hold_boolean is TRUE
            THEN 'ON Credit Hold'
          WHEN is_on_credit_hold_boolean is FALSE 
            THEN 'Not On Credit Hold' 
          ELSE 'Undefined' END
      AS is_on_credit_hold
  FROM dim_customer__handle_null
)

, dim_customer__flatten AS (
  SELECT
    dim_customer.*
    , customer_category.customer_category_name
    , buying_group.buying_group_name
  FROM dim_customer__convert_boolean dim_customer 
  LEFT JOIN `vit-lam-data.wide_world_importers.sales__customer_categories` customer_category
  ON dim_customer.customer_category_key = customer_category.customer_category_id
  LEFT JOIN `vit-lam-data.wide_world_importers.sales__buying_groups` buying_group 
  ON dim_customer.buying_group_key = buying_group.buying_group_id
)

SELECT
  customer_key
  , customer_name
  , customer_category_key
  , customer_category_name
  , buying_group_key
  , buying_group_name
  , is_on_credit_hold
FROM dim_customer__flatten

