WITH total_spend_customer AS (
  SELECT 
    customer_key
    , SUM(gross_amount) AS lifetime_sale_amount
    , SUM(
      IF(
        DATE_TRUNC(order_date, MONTH) = DATE_TRUNC((
          SELECT MAX(order_date) 
          FROM {{ref('fact_sales_order_line')}})
          , MONTH), gross_amount, 0
    )) AS last_month_sale_amount
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY customer_key
)

, total_spend_percentile AS (
  SELECT  
    *
    , PERCENT_RANK() OVER (ORDER BY lifetime_sale_amount) AS lifetime_monetary
    , PERCENT_RANK() OVER (ORDER BY last_month_sale_amount) AS last_month_monetary
  FROM total_spend_customer
)

SELECT
  customer_key
  , lifetime_sale_amount
  , CASE 
      WHEN lifetime_monetary BETWEEN 0.8 AND 1 THEN 'High'
      WHEN lifetime_monetary BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN lifetime_monetary BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Error'
    END AS lifetime_monetary
  , last_month_sale_amount
  , CASE 
      WHEN last_month_monetary BETWEEN 0.8 AND 1 THEN 'High'
      WHEN last_month_monetary BETWEEN 0.5 AND 0.8 THEN 'Medium'
      WHEN last_month_monetary BETWEEN 0 AND 0.5 THEN 'Low'
      ELSE 'Error'
    END AS last_month_monetary
FROM total_spend_percentile
