SELECT
  order_date AS metric_date,
  ROUND(SUM(order_amount_from_items), 2) AS gmv
FROM fct_orders
WHERE order_status = 'paid'
GROUP BY 1
ORDER BY 1;
