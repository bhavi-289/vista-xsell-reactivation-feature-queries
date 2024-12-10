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
  SELECT customer_id,
  food_first_order_date
  FROM
  (
      SELECT CAST(userid AS BIGINT) AS customer_id,
            MIN(DATE(first_order_date)) AS food_first_order_date
        FROM dev_bi.mot_first_order_date
      WHERE 1=1
        AND service IN ('food')
        AND city = 'Dubai'
        AND CAST(userid AS BIGINT) IN (SELECT customer_id FROM rh)
    GROUP BY 1
  )
  where
  food_first_order_date=DATE('{anchor_date}')
)
,quik AS (
  SELECT customer_id,
  quik_first_order_date
  from
  (
      SELECT CAST(userid AS BIGINT) AS customer_id,
             MIN(DATE(first_order_date)) AS quik_first_order_date
        FROM dev_bi.mot_first_order_date
       WHERE 1=1
         AND service IN ('quik')
         AND city = 'Dubai'
         AND CAST(userid AS BIGINT) IN (SELECT customer_id FROM rh)
    GROUP BY 1
  )
  where quik_first_order_date=DATE('{anchor_date}')
)
, customer_base AS (
  SELECT 
  food.customer_id,
  rh.rh_first_order_date,
  food.food_first_order_date,
  q.quik_first_order_date
  from food
  LEFT JOIN quik AS q
          ON q.customer_id = food.customer_id
  LEFT JOIN rh
          ON rh.customer_id = food.customer_id
  GROUP BY 1, 2, 3, 4
)
