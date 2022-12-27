WITH target_salesperson AS (
  SELECT 
    year_month
    , salesperson_person_key
    , target_gross_amount
  FROM
    {{ ref('fact_target_salesperson') }}
)

, fact_achievement AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , salesperson_person_key
    , SUM(gross_amount) AS achieved_gross_amount
  FROM 
    {{ ref('fact_sales_order_line') }}
  GROUP BY 
    DATE_TRUNC(order_date, MONTH)
    , salesperson_person_key
)

, fact_target_achieved_salesperson AS (
  SELECT
    target.year_month
    , target.salesperson_person_key
    , target.target_gross_amount
    , achieved.achieved_gross_amount
  FROM target_salesperson target
  FULL OUTER JOIN fact_achievement achieved
  ON target.year_month = achieved.year_month
  AND target.salesperson_person_key = achieved.salesperson_person_key
)

SELECT 
  year_month
  , salesperson_person_key
  , target_gross_amount
  , achieved_gross_amount
  , 100 * achieved_gross_amount / target_gross_amount AS achieved_percentage
  , CASE 
    WHEN 100 * achieved_gross_amount / target_gross_amount >= 95
      THEN "Achived"
    WHEN 100 * achieved_gross_amount / target_gross_amount < 95 OR 100 * achieved_gross_amount / target_gross_amount > 0
      THEN "Not Achived"
    ELSE "Error"
    END AS is_achieved
FROM fact_target_achieved_salesperson



