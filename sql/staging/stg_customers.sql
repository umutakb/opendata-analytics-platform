CREATE OR REPLACE VIEW stg_customers AS
SELECT
  CAST(customer_id AS BIGINT) AS customer_id,
  CAST(signup_date AS DATE) AS signup_date,
  UPPER(TRIM(country)) AS country,
  NULLIF(LOWER(TRIM(email)), '') AS email
FROM customers;
