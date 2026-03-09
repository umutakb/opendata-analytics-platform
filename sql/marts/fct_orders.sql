CREATE OR REPLACE TABLE fct_orders AS
WITH item_agg AS (
  SELECT
    order_id,
    ROUND(SUM(item_total), 2) AS order_amount_from_items,
    COUNT(*) AS item_lines
  FROM fct_order_items
  GROUP BY 1
)
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status,
  ROUND(o.total_amount, 2) AS order_total_amount,
  COALESCE(i.order_amount_from_items, 0.0) AS order_amount_from_items,
  ROUND(ROUND(o.total_amount, 2) - COALESCE(i.order_amount_from_items, 0.0), 2) AS reconciliation_delta,
  COALESCE(i.item_lines, 0) AS item_lines
FROM stg_orders o
LEFT JOIN item_agg i USING (order_id);
