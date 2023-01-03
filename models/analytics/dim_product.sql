WITH 
  dim_product__source AS (
    SELECT * 
    FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
  )
  
  , dim_product__rename_column AS (
    SELECT
      stock_item_id AS product_key
      , stock_item_name AS product_name
      , brand AS brand_name
      , size 
      , lead_time_days
      , quantity_per_outer
      , is_chiller_stock
      , barcode
      , tax_rate
      , unit_price
      , recommended_retail_price
      , typical_weight_per_unit
      , marketing_comments
      , internal_comments
      , photo
      , custom_fields
      , tags
      , search_details
      , supplier_id AS supplier_key
      , color_id AS color_key
      , unit_package_id AS unit_package_type_key
      , outer_package_id AS outer_package_type_key     
    FROM dim_product__source
  )

  , dim_product__cast_type AS (
    SELECT 
      CAST(product_key AS INTEGER) AS product_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(brand_name AS STRING) AS brand_name
      , CAST(size AS STRING) AS size
      , CAST(lead_time_days AS INTEGER) AS lead_time_days
      , CAST(quantity_per_outer AS INTEGER) AS quantity_per_outer
      , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
      , CAST(barcode AS STRING) AS barcode
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(recommended_retail_price AS NUMERIC) AS recommended_retail_price
      , CAST(typical_weight_per_unit AS NUMERIC) AS typical_weight_per_unit
      , CAST(marketing_comments AS STRING) AS marketing_comments
      , CAST(internal_comments AS STRING) AS internal_comments
      , CAST(photo AS BYTES) AS photo
      , CAST(custom_fields AS STRING) AS custom_fields
      , CAST(tags AS STRING) AS tags
      , CAST(search_details AS STRING) AS search_details
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(color_key AS INTEGER) AS color_key
      , CAST(unit_package_type_key AS INTEGER) AS unit_package_type_key
      , CAST(outer_package_type_key AS INTEGER) AS outer_package_type_key
    FROM dim_product__rename_column
  )

  , dim_product__handle_null AS (
    SELECT
      product_key
      , COALESCE(product_name, 'Undefined') AS product_name
      , COALESCE(brand_name, 'Undefined') AS brand_name
      , COALESCE(size, 'Undefined') AS size
      , lead_time_days 
      , quantity_per_outer
      , is_chiller_stock_boolean
      , COALESCE(barcode, 'Undefined') AS barcode 
      , tax_rate 
      , unit_price 
      , recommended_retail_price
      , typical_weight_per_unit
      , COALESCE(marketing_comments, 'Undefined') AS marketing_comments
      , COALESCE(internal_comments, 'Undefined') AS internal_comments
      , photo
      , COALESCE(custom_fields, 'Undefined') AS custom_fields
      , COALESCE(tags, 'Undefined') AS tags
      , COALESCE(search_details, 'Undefined') AS search_details
      , supplier_key
      , color_key
      , unit_package_type_key
      , outer_package_type_key
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
          ELSE 'Invalid' END
      AS is_chiller_stock
    FROM dim_product__handle_null dim_product
  )

  , dim_product__flatten AS (
      SELECT
        dim_product.*
        , CAST(dim_external_product.category_id AS INTEGER) AS category_key
        , CAST(dim_external_category.category_name AS STRING) AS category_name
        , CAST(dim_external_category.parent_category_id AS INTEGER) AS parent_category_key
        , CAST(dim_external_category.category_level AS INTEGER) AS category_level
        , COALESCE(dim_supplier.supplier_name, 'Undefined') AS supplier_name
        , COALESCE(dim_color.color_name, 'Undefined') AS color_name
        , COALESCE(dim_unit_package_type.package_type_name, 'Undefined') AS unit_package_type_name
        , COALESCE(dim_outer_package_type.package_type_name, 'Undefined') AS outer_package_type_name
      FROM dim_product__convert_boolean dim_product
      LEFT JOIN `vit-lam-data.wide_world_importers.purchasing__suppliers` dim_supplier
      ON dim_product.supplier_key = dim_supplier.supplier_id
      LEFT JOIN `vit-lam-data.wide_world_importers.warehouse__colors` dim_color
      ON dim_color.color_id = dim_product.color_key
      LEFT JOIN `tpp-learning`.`wide_world_importers_dwh`.`dim_package_type` dim_unit_package_type
      ON dim_unit_package_type.package_type_key = dim_product.unit_package_type_key
      LEFT JOIN `tpp-learning`.`wide_world_importers_dwh`.`dim_package_type` dim_outer_package_type
      ON dim_outer_package_type.package_type_key = dim_product.outer_package_type_key
      LEFT JOIN `vit-lam-data`.`wide_world_importers`.`external__stock_item` dim_external_product
      ON dim_external_product.stock_item_id = dim_product.product_key
      LEFT JOIN `vit-lam-data`.`wide_world_importers`.`external__categories` dim_external_category
      ON dim_external_category.category_id = dim_external_product.category_id
)

SELECT 
  product_key 
  , product_name 
  , brand_name 
  , size 
  , lead_time_days 
  , quantity_per_outer 
  , is_chiller_stock 
  , barcode 
  , tax_rate
  , unit_price 
  , recommended_retail_price 
  , typical_weight_per_unit 
  , marketing_comments 
  , internal_comments 
  , photo 
  , custom_fields 
  , tags 
  , search_details 
  , category_key
  , category_name
  , category_level
  , parent_category_key
  , supplier_key 
  , supplier_name 
  , color_key 
  , color_name 
  , unit_package_type_key 
  , unit_package_type_name 
  , outer_package_type_key 
  , outer_package_type_name 
FROM dim_product__flatten