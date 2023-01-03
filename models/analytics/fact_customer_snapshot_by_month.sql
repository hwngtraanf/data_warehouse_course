WITH fact_customer_snapshot_by_month__calculate_sale_amount AS (
  SELECT
    customer_key
    , DATE_TRUNC(order_date, MONTH) AS year_month
    , SUM(gross_amount) AS sale_amount
  FROM {{ ref('fact_sales_order_line') }}
  GROUP BY 
    customer_key
    , DATE_TRUNC(order_date, MONTH)
)

, dim_year_month AS (
  SELECT DISTINCT DATE_TRUNC(date, MONTH) as year_month
  FROM {{ ref('dim_date') }}
)

, distinct_customer_key AS (
  SELECT DISTINCT customer_key
  FROM {{ ref('fact_sales_order_line') }}
)

, customer_key_year_month AS (
  SELECT DISTINCT 
    customer_key
    , year_month
  FROM distinct_customer_key
  CROSS JOIN dim_year_month
  WHERE year_month BETWEEN 
    DATE_TRUNC((SELECT MIN(order_date) FROM {{ ref('fact_sales_order_line') }}), MONTH)
    AND DATE_TRUNC((SELECT MAX(order_date) FROM {{ ref('fact_sales_order_line') }}), MONTH)
)

, fact_customer_snapshot_by_month__calculate_sale_amount_all_months AS (
  SELECT
    key.customer_key
    , key.year_month
    , sale_amount.sale_amount
  FROM customer_key_year_month key
  LEFT JOIN fact_customer_snapshot_by_month__calculate_sale_amount sale_amount
  ON key.customer_key = sale_amount.customer_key 
  AND key.year_month = sale_amount.year_month
)

, fact_customer_snapshot_by_month__calculate_lifetime_sale_amount AS (
  SELECT
    *
    , SUM(sale_amount) OVER (PARTITION BY customer_key ORDER BY year_month) AS lifetime_sale_amount
  FROM fact_customer_snapshot_by_month__calculate_sale_amount_all_months
  ORDER BY 
    customer_key, year_month
)

, fact_customer_snapshot_by_month__calculate_percentile AS (
  SELECT 
    *
    , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY sale_amount) AS sale_amount_percentile
    , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY lifetime_sale_amount) AS lifetime_sale_amount_percentile
  FROM fact_customer_snapshot_by_month__calculate_lifetime_sale_amount
)

SELECT
  customer_key
  , year_month
  , sale_amount
  , CASE 
      WHEN sale_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
      WHEN sale_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN sale_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Error'
    END AS sale_amount_segment
  , lifetime_sale_amount
  , CASE 
      WHEN lifetime_sale_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
      WHEN lifetime_sale_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN lifetime_sale_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Error'
    END AS lifetime_sale_amount_segment
FROM fact_customer_snapshot_by_month__calculate_percentile
ORDER BY year_month

