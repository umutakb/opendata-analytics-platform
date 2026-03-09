CREATE OR REPLACE TABLE dim_customers AS
SELECT
  customer_id,
  signup_date,
  country,
  email
FROM stg_customers;
