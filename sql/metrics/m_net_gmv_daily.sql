SELECT
  order_date AS metric_date,
  ROUND(
    SUM(
      CASE
        WHEN order_status = 'paid' THEN order_amount_from_items
        WHEN order_status = 'refunded' THEN -order_amount_from_items
        ELSE 0
      END
    ),
    2
  ) AS net_gmv
FROM fct_orders
GROUP BY 1
ORDER BY 1;
