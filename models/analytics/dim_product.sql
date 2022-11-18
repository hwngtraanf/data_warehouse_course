SELECT 
  CAST(stock_item_id AS INTEGER) AS product_key
  , CAST(stock_item_name AS STIRNG) AS product_name
  , CAST(brand AS STIRNG) AS brand_name
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
