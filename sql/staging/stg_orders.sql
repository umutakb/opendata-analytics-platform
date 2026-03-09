CREATE OR REPLACE VIEW stg_orders AS
SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(customer_id AS BIGINT) AS customer_id,
  CAST(order_date AS DATE) AS order_date,
  CASE
    WHEN LOWER(TRIM(status)) IN ('paid') THEN 'paid'
    WHEN LOWER(TRIM(status)) IN ('canceled', 'cancelled') THEN 'canceled'
    WHEN LOWER(TRIM(status)) IN ('refunded') THEN 'refunded'
    ELSE 'unknown'
  END AS order_status,
  CAST(total_amount AS DOUBLE) AS total_amount
FROM orders;
