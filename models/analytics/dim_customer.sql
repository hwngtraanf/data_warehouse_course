WITH dim_customer__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_columns AS (
  SELECT
    customer_id AS customer_key
    , customer_name 
    , credit_limit
    , account_opened_date
    , standard_discount_percentage
    , is_statement_sent
    , is_on_credit_hold
    , payment_days
    , phone_number
    , fax_number
    , delivery_run
    , run_position
    , website_url
    , delivery_address_line_1
    , delivery_address_line_2
    , delivery_postal_code
    , postal_address_line_1
    , postal_address_line_2
    , postal_postal_code
    , bill_to_customer_id AS bill_to_customer_key
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
    , primary_contact_person_id AS primary_contact_person_key
    , alternate_contact_person_id AS alternate_contact_person_key
    , delivery_method_id AS delivery_method_key
    , delivery_city_id AS delivery_city_key
    , postal_city_id AS postal_city_key
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT
    CAST(customer_key AS INTEGER) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(credit_limit AS NUMERIC) AS credit_limit
    , CAST(account_opened_date AS DATE) AS account_opened_date
    , CAST(standard_discount_percentage AS NUMERIC) AS standard_discount_percentage
    , CAST(is_statement_sent AS BOOLEAN) AS is_statement_sent_boolean
    , CAST(is_on_credit_hold AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST(payment_days AS INTEGER) AS payment_days
    , CAST(phone_number AS STRING) AS phone_number
    , CAST(fax_number AS STRING) AS fax_number
    , CAST(delivery_run AS STRING) AS delivery_run
    , CAST(run_position AS STRING) AS run_position
    , CAST(website_url AS STRING) AS website_url
    , CAST(delivery_address_line_1 AS STRING) AS delivery_address_line_1
    , CAST(delivery_address_line_2 AS STRING) AS delivery_address_line_2
    , CAST(delivery_postal_code AS STRING) AS delivery_postal_code
    , CAST(postal_address_line_1 AS STRING) AS postal_address_line_1
    , CAST(postal_address_line_2 AS STRING) AS postal_address_line_2
    , CAST(postal_postal_code AS STRING) AS postal_postal_code
    , CAST(bill_to_customer_key AS INTEGER) AS bill_to_customer_key
    , CAST(customer_category_key AS INTEGER) AS customer_category_key
    , CAST(buying_group_key AS INTEGER) AS buying_group_key
    , CAST(primary_contact_person_key AS INTEGER) AS primary_contact_person_key
    , CAST(alternate_contact_person_key AS INTEGER) AS alternate_contact_person_key
    , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
    , CAST(delivery_city_key AS INTEGER) AS delivery_city_key
    , CAST(postal_city_key AS INTEGER) AS postal_city_key
  FROM dim_customer__rename_columns
)

, dim_customer__handle_null AS (
  SELECT
    customer_key
    , COALESCE(customer_name, 'Undefined') AS customer_name
    , credit_limit
    , account_opened_date
    , standard_discount_percentage
    , is_statement_sent_boolean
    , is_on_credit_hold_boolean
    , payment_days
    , COALESCE(phone_number, 'Undefined') AS phone_number 
    , COALESCE(fax_number, 'Undefined') AS fax_number  
    , COALESCE(delivery_run, 'Undefined') AS delivery_run  
    , COALESCE(run_position, 'Undefined') AS run_position  
    , COALESCE(website_url, 'Undefined') AS website_url  
    , COALESCE(delivery_address_line_1, 'Undefined') AS delivery_address_line_1  
    , COALESCE(delivery_address_line_2, 'Undefined') AS delivery_address_line_2  
    , COALESCE(delivery_postal_code, 'Undefined') AS delivery_postal_code  
    , COALESCE(postal_address_line_1, 'Undefined') AS postal_address_line_1  
    , COALESCE(postal_address_line_2, 'Undefined') AS postal_address_line_2  
    , COALESCE(postal_postal_code, 'Undefined') AS postal_postal_code  
    , bill_to_customer_key
    , customer_category_key
    , buying_group_key
    , primary_contact_person_key
    , alternate_contact_person_key
    , delivery_method_key
    , delivery_city_key
    , postal_city_key
  FROM dim_customer__cast_type
  )

, dim_customer__convert_boolean AS (
  SELECT
    customer_key
    , customer_name
    , credit_limit
    , account_opened_date
    , standard_discount_percentage
    , CASE 
          WHEN is_statement_sent_boolean is TRUE
            THEN 'Statement Sent'
          WHEN is_statement_sent_boolean is FALSE 
            THEN 'Statement Not Sent' 
          ELSE 'Undefined' END
      AS is_statement_sent
    , CASE 
          WHEN is_on_credit_hold_boolean is TRUE
            THEN 'ON Credit Hold'
          WHEN is_on_credit_hold_boolean is FALSE 
            THEN 'Not On Credit Hold' 
          ELSE 'Undefined' END
      AS is_on_credit_hold
    , payment_days
    , phone_number
    , fax_number
    , delivery_run
    , run_position
    , website_url
    , delivery_address_line_1
    , delivery_address_line_2
    , delivery_postal_code
    , postal_address_line_1
    , postal_address_line_2
    , postal_postal_code
    , bill_to_customer_key
    , customer_category_key
    , buying_group_key
    , primary_contact_person_key
    , alternate_contact_person_key
    , delivery_method_key
    , delivery_city_key
    , postal_city_key 
  FROM dim_customer__handle_null
)

, dim_customer__flatten AS (
  SELECT
    dim_customer.*
    , COALESCE(customer_category.customer_category_name, 'Undefined') AS customer_category_name 
    , COALESCE(buying_group.buying_group_name, 'Undefined') AS buying_group_name
    , COALESCE(primary_contact.full_name, 'Undefined') AS primary_contact_person_name
    , COALESCE(alternative_contact.full_name, 'Undefined') AS alternate_contact_person_name
    , COALESCE(customer.customer_name, 'Undefined') AS bill_to_customer_name
    , COALESCE(delivery_method.delivery_method_name, 'Undefined') AS delivery_method_name
    , COALESCE(delivery_cities.city_name, 'Undefined') AS delivery_city_name
    , delivery_state_provinces.state_province_id AS delivery_state_province_key
    , COALESCE(delivery_state_provinces.state_province_name, 'Undefined') AS delivery_state_province_name
    , COALESCE(postal_cities.city_name, 'Undefined') AS postal_city_name
  FROM dim_customer__convert_boolean dim_customer
  LEFT JOIN `vit-lam-data.wide_world_importers.sales__customer_categories` customer_category
  ON dim_customer.customer_category_key = customer_category.customer_category_id
  LEFT JOIN `vit-lam-data.wide_world_importers.sales__buying_groups` buying_group 
  ON dim_customer.buying_group_key = buying_group.buying_group_id
  LEFT JOIN `vit-lam-data.wide_world_importers.application__people` primary_contact 
  ON dim_customer.primary_contact_person_key = primary_contact.person_id
  LEFT JOIN `vit-lam-data.wide_world_importers.application__people` alternative_contact 
  ON dim_customer.primary_contact_person_key = alternative_contact.person_id
  LEFT JOIN `vit-lam-data.wide_world_importers.sales__customers` customer 
  ON dim_customer.bill_to_customer_key = customer.customer_id
  LEFT JOIN `vit-lam-data.wide_world_importers.application__delivery_methods` delivery_method 
  ON dim_customer.delivery_method_key = delivery_method.delivery_method_id
  LEFT JOIN `vit-lam-data.wide_world_importers.application__cities` delivery_cities 
  ON dim_customer.delivery_city_key = delivery_cities.city_id
  LEFT JOIN `vit-lam-data.wide_world_importers.application__state_provinces` delivery_state_provinces 
  ON delivery_state_provinces.state_province_id = delivery_cities.state_province_id  
  LEFT JOIN `vit-lam-data.wide_world_importers.application__cities` postal_cities 
  ON dim_customer.delivery_city_key = postal_cities.city_id
)

SELECT
  customer_key
  , customer_name
  , credit_limit
  , account_opened_date
  , standard_discount_percentage
  , is_statement_sent
  , is_on_credit_hold
  , payment_days
  , phone_number
  , fax_number
  , delivery_run
  , run_position
  , website_url
  , delivery_address_line_1
  , delivery_address_line_2
  , delivery_postal_code
  , postal_address_line_1
  , postal_address_line_2
  , postal_postal_code
  , customer_category_key
  , customer_category_name
  , buying_group_key
  , buying_group_name
  , primary_contact_person_key
  , primary_contact_person_name
  , alternate_contact_person_key
  , alternate_contact_person_name
  , bill_to_customer_key
  , bill_to_customer_name
  , delivery_method_key
  , delivery_method_name
  , delivery_city_key
  , delivery_city_name
  , delivery_state_province_key
  , delivery_state_province_name
  , postal_city_key
  , postal_city_name
FROM dim_customer__flatten

