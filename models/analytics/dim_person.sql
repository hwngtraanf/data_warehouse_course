WITH dim_person__source AS (
  SELECT
    *
  FROM
    `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_columns AS (
  SELECT
    person_id AS person_key
    , full_name
    , preferred_name
    , search_name
    , is_permitted_to_logon 
    , logon_name
    , is_external_logon_provider
    , hashed_password
    , is_system_user
    , is_employee
    , is_salesperson
    , user_preferences
    , phone_number
    , fax_number
    , email_address
    , photo
    , custom_fields
    , other_languages
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT 
    CAST(person_key AS INTEGER) AS person_key
    , CAST(full_name AS STRING) AS full_name
    , CAST(preferred_name AS STRING) AS preferred_name
    , CAST(search_name AS STRING) AS search_name
    , CAST(is_permitted_to_logon AS BOOLEAN) AS is_permitted_to_logon_boolean
    , CAST(logon_name AS STRING) AS logon_name
    , CAST(is_external_logon_provider AS BOOLEAN) AS is_external_logon_provider_boolean
    , CAST(hashed_password AS BYTES) AS hashed_password
    , CAST(is_system_user AS BOOLEAN) AS is_system_user_boolean
    , CAST(is_employee AS BOOLEAN) AS is_employee_boolean
    , CAST(is_salesperson AS BOOLEAN) AS is_salesperson_boolean
    , CAST(user_preferences AS STRING) AS user_preferences
    , CAST(phone_number AS STRING) AS phone_number
    , CAST(fax_number AS STRING) AS fax_number
    , CAST(email_address AS STRING) AS email_address
    , CAST(photo AS BYTES) AS photo
    , CAST(custom_fields AS STRING) AS custom_fields
    , CAST(other_languages AS STRING) AS other_languages
  FROM dim_person__rename_columns
)

SELECT 
  person_key
  , COALESCE(full_name, "Undefined") AS full_name
  , COALESCE(preferred_name, "Undefined") AS preferred_name
  , COALESCE(search_name, "Undefined") AS search_name
  , CASE 
      WHEN is_permitted_to_logon_boolean is TRUE
        THEN 'Permitted To Login'
      WHEN is_permitted_to_logon_boolean is FALSE 
        THEN 'Not Permitted To Login' 
      ELSE 'Invalid' END
  AS is_permitted_to_logon
  , hashed_password
  , CASE 
      WHEN is_system_user_boolean is TRUE
        THEN 'System User'
      WHEN is_system_user_boolean is FALSE 
        THEN 'Not System User' 
      ELSE 'Invalid' END
  AS is_system_user
  , CASE 
      WHEN is_employee_boolean is TRUE
        THEN 'Employee'
      WHEN is_employee_boolean is FALSE 
        THEN 'Not Employee' 
      ELSE 'Invalid' END
  AS is_employee
  , CASE 
      WHEN is_salesperson_boolean is TRUE
        THEN 'Salesperson'
      WHEN is_salesperson_boolean is FALSE 
        THEN 'Not Salesperson' 
      ELSE 'Invalid' END
  AS is_salesperson
  , COALESCE(user_preferences, "Undefined") AS user_preferences
  , COALESCE(phone_number, "Undefined") AS phone_number
  , COALESCE(fax_number, "Undefined") AS fax_number
  , COALESCE(email_address, "Undefined") AS email_address
  , photo
  , COALESCE(custom_fields, "Undefined") AS custom_fields
  , COALESCE(other_languages, "Undefined") AS other_languages
FROM dim_person__cast_type

UNION ALL 

SELECT 
  0 AS person_key
  , "Undefined" AS full_name
  , "Undefined"
  , "Undefined"
  , "Undefined"
  , NULL
  , "Undefined"
  , "Undefined"
  , "Undefined"
  , "Undefined"
  , "Undefined"
  , "Undefined"
  , "Undefined"
  , NULL
  , "Undefined"
  , "Undefined"

UNION ALL

SELECT 
  -1 AS person_key
  , "Error" AS full_name
  , "Error"
  , "Error"
  , "Error"
  , NULL
  , "Error"
  , "Error"
  , "Error"
  , "Error"
  , "Error"
  , "Error"
  , "Error"
  , NULL
  , "Error"
  , "Error"