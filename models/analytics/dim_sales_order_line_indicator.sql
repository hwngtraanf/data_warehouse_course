WITH dim_is_undersupply_backordered AS (
  SELECT
    TRUE AS is_undersupply_backordered_boolean
    , 'Undersupply Backordered' AS is_undersupply_backordered

  UNION ALL

  SELECT
    FALSE AS is_undersupply_backordered_boolean
    , 'Not Undersupply Backordered' AS is_undersupply_backordered

)
SELECT 
  *
  , CONCAT(dim_undersupply_backordered.is_undersupply_backordered_boolean, ',', dim_package_type.package_type_key) AS sales_order_line_indicator_key
FROM dim_is_undersupply_backordered dim_undersupply_backordered
CROSS JOIN `tpp-learning.wide_world_importers_dwh.dim_package_type` dim_package_type