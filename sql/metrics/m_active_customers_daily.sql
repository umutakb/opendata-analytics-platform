SELECT
  order_date AS metric_date,
  COUNT(DISTINCT customer_id) AS active_customers
FROM fct_orders
WHERE order_status = 'paid'
GROUP BY 1
ORDER BY 1;
