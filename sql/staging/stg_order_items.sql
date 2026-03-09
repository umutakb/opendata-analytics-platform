CREATE OR REPLACE VIEW stg_order_items AS
SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(product_id AS BIGINT) AS product_id,
  CAST(quantity AS INTEGER) AS quantity,
  CAST(unit_price AS DOUBLE) AS unit_price
FROM order_items;
