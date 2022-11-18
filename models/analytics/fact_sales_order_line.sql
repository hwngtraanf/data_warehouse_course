SELECT 
  CAST(order_line_id AS INTEGER) AS sales_order_line_key
  , CAST(stock_item_id AS INTEGER) AS product_key
  , CAST(quantity AS INTEGER) AS quantity
  , CAST(unit_price AS NUMERIC) AS unit_price
  , CAST(quantity AS INTEGER) * CAST(unit_price AS NUMERIC) AS gross_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`
