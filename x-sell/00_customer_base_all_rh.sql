WITH  
valid_users AS
(
    SELECT CAST(customer_id AS BIGINT) AS customer_id
      FROM prod_dwh.trip
     WHERE 1=1
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
       AND service_area_id = 1
       AND trip_outcome = 3
       AND pick_up_latitude > 0
       AND pick_up_longitude > 0
       AND drop_off_latitude > 0
       AND drop_off_longitude > 0
       AND captain_id IS NOT NULL
       AND customer_id IS NOT NULL
       AND booking_id IS NOT NULL
       AND is_valid = 1
       AND LOWER(business_type) IN ('jv', 'hala', 'ride-hailing', 'ridehailing', 'ride hailing')
       AND LOWER(cct) NOT LIKE '%test%'
       AND cct_id NOT IN (1716, 1474, 1279, 1536, 1067, 368, 1723, 465, 1392)
  GROUP BY 1
)
,rh AS (
    SELECT CAST(userid AS BIGINT) AS customer_id, 
           MIN(DATE(first_order_date)) AS rh_first_order_date
      FROM dev_bi.mop_first_order_date
     WHERE 1=1
       AND service IN ('ride_hailing', 'limo')
       AND city = 'Dubai'
       AND CAST(userid AS BIGINT) IN (SELECT customer_id FROM valid_users)
  GROUP BY 1
)
,food AS (
    SELECT CAST(userid AS BIGINT) AS customer_id,
           MIN(DATE(first_order_date)) AS food_first_order_date
      FROM dev_bi.mot_first_order_date
     WHERE 1=1
       AND service IN ('food')
       AND city = 'Dubai'
       AND CAST(userid AS BIGINT) IN (SELECT customer_id FROM rh)
  GROUP BY 1
)
,quik AS (
    SELECT CAST(userid AS BIGINT) AS customer_id,
           MIN(DATE(first_order_date)) AS quik_first_order_date
      FROM dev_bi.mot_first_order_date
     WHERE 1=1
       AND service IN ('quik')
       AND city = 'Dubai'
       AND CAST(userid AS BIGINT) IN (SELECT customer_id FROM rh)
  GROUP BY 1
)
,customer_base AS (
    SELECT rh.customer_id,
           rh.rh_first_order_date,
           f.food_first_order_date,
           q.quik_first_order_date,
           CASE
               WHEN f.food_first_order_date IS NULL AND q.quik_first_order_date IS NULL AND rh.rh_first_order_date < DATE('{anchor_date}') THEN 'rh-only'
               WHEN f.food_first_order_date IS NOT NULL AND q.quik_first_order_date IS NULL AND f.food_first_order_date >= DATE('{anchor_date}') AND f.food_first_order_date < DATE('{target_end_date}') THEN 'rh-food'
               WHEN f.food_first_order_date IS NULL AND q.quik_first_order_date IS NOT NULL AND q.quik_first_order_date >= DATE('{anchor_date}') AND q.quik_first_order_date < DATE('{target_end_date}') THEN 'rh-quik'
               WHEN f.food_first_order_date IS NOT NULL AND q.quik_first_order_date IS NOT NULL AND f.food_first_order_date >= DATE('{anchor_date}') AND f.food_first_order_date < DATE('{target_end_date}') AND q.quik_first_order_date >= DATE('{anchor_date}') AND q.quik_first_order_date < DATE('{target_end_date}') THEN 'rh-food-quik'
               ELSE NULL
           END AS service
      FROM rh
 LEFT JOIN food AS f
        ON f.customer_id = rh.customer_id
 LEFT JOIN quik AS q
        ON q.customer_id = rh.customer_id
  GROUP BY 1, 2, 3, 4, 5
)