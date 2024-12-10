,cplus_active_days AS (
    SELECT CAST(customer_id AS BIGINT) AS customer_id,
           COUNT(DISTINCT day) AS active_days
      FROM dev_pricing.careem_plus_active_subscriptions
     WHERE 1=1
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
  GROUP BY customer_id
)
,cplus_status AS (-- find a better threshold and aligned with team
    SELECT u.customer_id,
           CASE
               WHEN COALESCE(c.active_days, 0) >= {cplus_active_days_threshold} THEN 'Cplus Active'
               ELSE 'Cplus Inactive'
           END AS cplus_status
      FROM customer_base u
 LEFT JOIN cplus_active_days c
        ON u.customer_id = c.customer_id
)
    SELECT customer_id,
           cplus_status
      FROM cplus_status