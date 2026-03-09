CREATE OR REPLACE TABLE fct_order_items AS
SELECT
  order_id,
  product_id,
  quantity,
  unit_price,
  ROUND(quantity * unit_price, 2) AS item_total
FROM stg_order_items;
