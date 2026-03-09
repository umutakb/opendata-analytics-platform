CREATE OR REPLACE TABLE dim_products AS
SELECT
  product_id,
  category,
  created_at
FROM stg_products;
