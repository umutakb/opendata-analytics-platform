WITH daily AS (
  SELECT
    order_date AS metric_date,
    SUM(order_amount_from_items) AS gmv,
    COUNT(*) AS orders
  FROM fct_orders
  WHERE order_status = 'paid'
  GROUP BY 1
)
SELECT
  metric_date,
  ROUND(gmv, 2) AS gmv,
  orders,
  ROUND(gmv / NULLIF(orders, 0), 2) AS aov
FROM daily
ORDER BY 1;
