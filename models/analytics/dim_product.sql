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
    FROM dim_product__source
  )

  , dim_product__cast_type AS (
    SELECT 
      CAST(product_key AS INTEGER) AS product_key
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(brand_name AS STRING) AS brand_name
    FROM dim_product__rename_column
  )

  , dim_product__join_dim_supplier AS (
      SELECT
        dim_product.*
        , dim_supplier.supplier_name
      FROM dim_product__cast_type dim_product
      LEFT JOIN {{ ref('dim_supplier') }} dim_supplier
      ON dim_product.supplier_key = dim_supplier.supplier_key
)

SELECT 
  product_key
  , supplier_key
  , product_name
  , supplier_name
  , brand_name
FROM dim_product__join_dim_supplier
