SELECT 
  order_line_id AS sales_order_line_key
  , stock_item_id as product_key
  , quantity
  , unit_price
  , quantity * unit_price AS gross_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`
