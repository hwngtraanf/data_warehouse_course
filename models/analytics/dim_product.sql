WITH 
  dim_product__source AS (
    SELECT * 
    FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
  )
  
  , dim_product__rename_column AS (
    SELECT
      stock_item_id AS product_key
      , supplier_id AS supplier_key
      , stock_item_name AS product_name
      , brand AS brand_name
      , is_chiller_stock
    FROM dim_product__source
  )

  , dim_product__cast_type AS (
    SELECT 
      CAST(product_key AS INTEGER) AS product_key
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(brand_name AS STRING) AS brand_name
      , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
    FROM dim_product__rename_column
  )

  , dim_product__handle_null AS (
    SELECT
      product_key
      , supplier_key
      , COALESCE(product_name, 'Undefined') AS product_name
      , COALESCE(brand_name, 'Undefined') AS brand_name
      , is_chiller_stock_boolean
    FROM dim_product__cast_type
  )

  , dim_product__convert_boolean AS (
    SELECT 
      dim_product.*
      , CASE 
          WHEN is_chiller_stock_boolean is TRUE
            THEN 'Chiller Stock'
          WHEN is_chiller_stock_boolean is FALSE 
            THEN 'Not Chiller Stock' 
          ELSE 'Undefined' END
      AS is_chiller_stock
    FROM dim_product__handle_null dim_product
  )

  , dim_product__join_dim_supplier AS (
      SELECT
        dim_product.*
        , COALESCE(dim_supplier.supplier_name, 'Undefined') AS supplier_name
      FROM dim_product__convert_boolean dim_product
      LEFT JOIN `tpp-learning`.`wide_world_importers_dwh`.`dim_supplier` dim_supplier
      ON dim_product.supplier_key = dim_supplier.supplier_key
)

SELECT 
  product_key
  , supplier_key
  , product_name
  , supplier_name
  , brand_name
  , is_chiller_stock
FROM dim_product__join_dim_supplier