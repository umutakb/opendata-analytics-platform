SELECT
  order_date AS metric_date,
  COUNT(*) AS orders
FROM fct_orders
WHERE order_status = 'paid'
GROUP BY 1
ORDER BY 1;
