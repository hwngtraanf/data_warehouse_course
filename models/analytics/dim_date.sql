SELECT
  d AS date
  , FORMAT_DATE('%A', d) AS day_of_week
  , FORMAT_DATE('%a', d) AS day_of_week_short
  , CASE 
      WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 'Weekend' ELSE 'Weekday' 
    END AS is_weekday_or_weekend
  , DATE_TRUNC(d, MONTH) AS year_month
  , FORMAT_DATE('%B', d) AS month
  , DATE_TRUNC(d, YEAR) AS year
  , EXTRACT(YEAR FROM d) AS year_number
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2012-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d )