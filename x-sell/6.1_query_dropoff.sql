,
bookings AS (
    SELECT CAST(t.customer_id AS BIGINT) AS customer_id,
           CAST(t.booking_id AS BIGINT) AS booking_id,
         --CAST(t.drop_off_time AS TIMESTAMP) AS drop_off_time,
           ROUND(CAST(t.drop_off_latitude AS DOUBLE),3) AS latitude,
           ROUND(CAST(t.drop_off_longitude AS DOUBLE),3) AS longitude--,
        --t.day
      FROM prod_dwh.trip t
     WHERE 1=1
       AND t.day >= DATE('{source_start_date}')
       AND t.day < DATE('{anchor_date}')
       AND t.service_area_id = 1
       AND t.trip_outcome = 3
       AND t.pick_up_latitude > 0
       AND t.pick_up_longitude > 0
       AND t.drop_off_latitude > 0
       AND t.drop_off_longitude > 0
       AND t.captain_id IS NOT NULL
       AND t.customer_id IS NOT NULL
       AND t.booking_id IS NOT NULL
       AND t.is_valid = 1
      AND CAST(t.customer_id AS BIGINT) in (select customer_id from customer_base)
       AND LOWER(t.business_type) IN ('jv', 'hala', 'ride-hailing', 'ridehailing', 'ride hailing')
       AND LOWER(t.cct) NOT LIKE '%test%'
       AND t.cct_id NOT IN (1716, 1474, 1279, 1536, 1067, 368, 1723, 465, 1392)
  GROUP BY 1,2,3,4
    )
SELECT b.customer_id,
       b.latitude,
       b.longitude,
       COUNT(*) AS trip_cnts
FROM bookings b
GROUP BY 1,2,3