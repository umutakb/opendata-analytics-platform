WITH paid_orders AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', order_date) AS order_month
  FROM fct_orders
  WHERE order_status = 'paid'
),
first_paid AS (
  SELECT
    customer_id,
    MIN(order_month) AS cohort_month
  FROM paid_orders
  GROUP BY 1
),
cohort_activity AS (
  SELECT
    fp.cohort_month,
    DATE_DIFF('month', fp.cohort_month, po.order_month) AS month_n,
    po.customer_id
  FROM first_paid fp
  JOIN paid_orders po USING (customer_id)
),
cohort_size AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size
  FROM first_paid
  GROUP BY 1
)
SELECT
  ca.cohort_month::DATE AS cohort_month,
  ca.month_n,
  COUNT(DISTINCT ca.customer_id) AS retained_customers,
  cs.cohort_size,
  ROUND(COUNT(DISTINCT ca.customer_id) * 1.0 / NULLIF(cs.cohort_size, 0), 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_size cs USING (cohort_month)
GROUP BY 1, 2, 4
ORDER BY 1, 2;
