CREATE OR REPLACE VIEW stg_products AS
SELECT
  CAST(product_id AS BIGINT) AS product_id,
  TRIM(category) AS category,
  CAST(created_at AS DATE) AS created_at
FROM products;
