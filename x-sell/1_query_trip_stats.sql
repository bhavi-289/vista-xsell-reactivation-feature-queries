,cplus_promo_usage AS ( 
    SELECT booking_id
      FROM prod_dwh.wallet_credit_details
     WHERE 1=1
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
       AND LOWER(credit_title) = 'marketing credits'
       AND LOWER(credit_type) IN ('careemplus','careemplushala', 'careemplusamman', 'careemplus2','careemplushala2','careemplusnbdhala','careemplusnbd','careemplusnbdhala')
)
,trips AS (-- need to figure out what time range to take to gather customer preference data to remove seasonality
    SELECT CAST(customer_id AS BIGINT) AS customer_id,
           t.booking_id AS order_id,
           booking_date AS order_received_timestamp,
           date_diff('day', booking_date, date('{anchor_date}')) as days_from_anchor,
           LOWER(business_type) AS business_type,
           booking_platform,
           service_area_id,
           LOWER(cct) AS cct,
           CAST(booking_date AS DATE) AS date,
           t.distance_travelled,
           t.duration_time,
           t.trip_price AS revenue,
           CASE
               WHEN HOUR(booking_date + INTERVAL '4' HOUR) >= 6 AND HOUR(booking_date + INTERVAL '4' HOUR) < 10 THEN 'Morning'
               WHEN HOUR(booking_date + INTERVAL '4' HOUR) >= 10 AND HOUR(booking_date + INTERVAL '4' HOUR) < 16 THEN 'Day'
               WHEN HOUR(booking_date + INTERVAL '4' HOUR) >= 16 AND HOUR(booking_date + INTERVAL '4' HOUR) < 20 THEN 'Evening'
               ELSE 'Night'
           END AS time_of_day,
           TRIM(BOTH ' ' FROM LOWER(customer_payment_selection)) AS payment_type,
           CASE WHEN c.booking_id IS NOT NULL then '' ELSE booking_promo_code END as booking_promo_code,
           CASE WHEN c.booking_id IS NOT NULL THEN 'cplus_promo_used' else 'non_cplus_promo_used' END as cplus_promo_usage
      FROM prod_dwh.trip t
 LEFT JOIN cplus_promo_usage c
        ON c.booking_id = t.booking_id
     WHERE 1=1
       AND CAST(t.customer_id AS BIGINT) in (select customer_id from customer_base)
       AND t.day >= DATE('{source_start_date}')
       AND t.day < DATE('{anchor_date}')
       AND service_area_id = 1
       AND t.trip_outcome = 3
       AND t.pick_up_latitude > 0
       AND t.pick_up_longitude > 0
       AND t.drop_off_latitude > 0
       AND t.drop_off_longitude > 0
       AND t.captain_id IS NOT NULL
       AND t.customer_id IS NOT NULL
       AND t.booking_id IS NOT NULL
       AND t.is_valid = 1
       AND lower(t.business_type) IN ('jv', 'hala', 'ride-hailing', 'ridehailing', 'ride hailing')
       AND lower(cct) NOT LIKE '%test%'
       AND cct_id NOT IN (1716, 1474, 1279, 1536, 1067, 368, 1723, 465, 1392)
 
)
,base AS (
    SELECT DISTINCT customer_id
      FROM trips
)
,preferred_cct AS (-- checking if count of overall trips is low and there is no clear distinction between preference then to leave null
    SELECT customer_id,
           cct AS preferred_cct
      FROM (
               SELECT customer_id,
                      cct,
                      COUNT(*) AS cnt,
                      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
                 FROM trips
             GROUP BY 1,2
           ) AS ranked_cct
     WHERE rn = 1
       AND cnt >= {fav_cct_min_trip_cnt}
)
,ride_usage AS (
    SELECT customer_id,
           COUNT(*) AS total_rides
      FROM trips
  GROUP BY 1
)
,ride_usage_classification AS (
    SELECT customer_id,
           CASE
               WHEN total_rides <= 3 THEN 'Low'
               WHEN total_rides BETWEEN 4 AND 9 THEN 'Medium'
               WHEN total_rides BETWEEN 10 AND 24 THEN 'High'
               ELSE 'Power'
           END AS ride_usage_behaviour
      FROM ride_usage
)
,time_of_day_commuter AS (-- need to put a check on if 2 time of days have the same % then which to assign to, or assign a min count to enter null
    SELECT customer_id,
           CASE
               WHEN MAX(CASE WHEN time_of_day = 'Morning' THEN percentage ELSE 0 END) > MAX(CASE WHEN time_of_day = 'Day' THEN percentage ELSE 0 END) AND
                    MAX(CASE WHEN time_of_day = 'Morning' THEN percentage ELSE 0 END) > MAX(CASE WHEN time_of_day = 'Evening' THEN percentage ELSE 0 END) AND
                    MAX(CASE WHEN time_of_day = 'Morning' THEN percentage ELSE 0 END) > MAX(CASE WHEN time_of_day = 'Night' THEN percentage ELSE 0 END)
                    THEN 'Morning Commuter'
               WHEN MAX(CASE WHEN time_of_day = 'Day' THEN percentage ELSE 0 END) > MAX(CASE WHEN time_of_day = 'Evening' THEN percentage ELSE 0 END) AND
                    MAX(CASE WHEN time_of_day = 'Day' THEN percentage ELSE 0 END) > MAX(CASE WHEN time_of_day = 'Night' THEN percentage ELSE 0 END)
                    THEN 'Day Commuter'
               WHEN MAX(CASE WHEN time_of_day = 'Evening' THEN percentage ELSE 0 END) > MAX(CASE WHEN time_of_day = 'Night' THEN percentage ELSE 0 END)
                    THEN 'Evening Commuter'
               ELSE 'Night Commuter'
           END AS time_of_day_commuter
      FROM (
               SELECT customer_id,
                      time_of_day,
                      COUNT(*) AS cnt,
                      (COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY customer_id)) * 100 AS percentage
                 FROM trips
             GROUP BY 1, 2
           ) AS time_of_day_stats
     WHERE cnt >= {fav_commute_time_min_trip_cnt}
  GROUP BY 1
)
,preferred_payment_method AS (-- see if the min count is low and if same number for top ranked then may want to pass as null/ there is another table that has this payment type extensive data
    SELECT customer_id,
           payment_type AS preferred_payment_method
      FROM (
               SELECT customer_id,
                      payment_type,
                      COUNT(*) AS cnt,
                      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
                 FROM trips
             GROUP BY 1,2
           ) AS ranked_payment
     WHERE rn = 1
       AND cnt >= {fav_payment_min_trip_cnt}
)
,parent_status AS (-- to be discussed
    SELECT customer_id,
           CASE
               WHEN MAX(CASE WHEN cct IN ('school rides', 'kids', 'careem kids', 'hala kids', 'hala juniors') THEN percentage ELSE 0 END) >= {parent_min_trip_cnt} THEN 'Parent'
               ELSE 'Non-Parent'
           END AS parent_status
      FROM (
               SELECT customer_id,
                      cct,
                      COUNT(*) AS cnt,
                      (COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY customer_id)) * 100 AS percentage
                 FROM trips
             GROUP BY 1,2
           ) AS cct_stats
  GROUP BY 1
)
,promo_behaviour AS (
    SELECT customer_id,
    (SUM(CASE WHEN booking_promo_code IS NOT NULL AND booking_promo_code <> '' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 as promo_perc,
           CASE
               WHEN (SUM(CASE WHEN booking_promo_code IS NOT NULL AND booking_promo_code <> '' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 > {promo_percent_threshold} THEN 'Promo Seeker'
               ELSE 'Non-Promo Seeker'
           END AS promo_behaviour
      FROM trips
  GROUP BY 1
)
,avg_revenue_data AS (
    SELECT t.customer_id,
           MAX(days_from_anchor) as days_since_least_recent_trip,
           MIN(days_from_anchor) as days_since_most_recent_trip,
           ROUND(AVG(t.distance_travelled),2) AS avg_distance,
           ROUND(AVG(t.duration_time),2) AS avg_duration,
           ROUND(AVG(t.revenue),2) AS avg_revenue,
           COUNT(*) AS trip_count,
           SUM(t.revenue) as trips_total_gmv,
           SUM(EXP(-days_from_anchor / 90.0)) AS exp_weight_90,
           SUM(EXP(-days_from_anchor / 60.0)) AS exp_weight_60,
           SUM(EXP(-days_from_anchor / 30.0)) AS exp_weight_30,
           SUM(EXP(-days_from_anchor / 10.0)) AS exp_weight_10,
           SUM(EXP(-days_from_anchor / 5.0)) AS exp_weight_5
      FROM trips t
  GROUP BY t.customer_id
)
,spend_bucket AS (
    SELECT customer_id,
           CASE
               WHEN avg_revenue > 25 THEN 'high'
               WHEN avg_revenue BETWEEN 15 AND 24 THEN 'medium'
               ELSE 'low'
           END AS spend_bucket,
           CASE
               WHEN avg_distance > 16 THEN 'high'
               WHEN avg_distance BETWEEN 12 AND 16 THEN 'medium'
               WHEN avg_distance < 12 THEN 'low'
           END AS distance_bucket,
           CASE
               WHEN avg_duration > 20 THEN 'high'
               WHEN avg_duration BETWEEN 14 AND 20 THEN 'medium'
               WHEN avg_duration < 14 THEN 'low'
           END AS duration_bucket,
           CASE
               WHEN trip_count > 8 THEN 'high'
               WHEN trip_count BETWEEN 5 AND 8 THEN 'medium'
               WHEN trip_count < 5 THEN 'low'
           END AS trip_bucket
      FROM avg_revenue_data
)
    SELECT b.customer_id,
           c.preferred_cct,
           r.total_rides,
           u.ride_usage_behaviour,
           t.time_of_day_commuter,
           p.preferred_payment_method,
           m.parent_status,
           f.promo_perc,
           f.promo_behaviour,
           s.spend_bucket,
           s.distance_bucket,
           s.duration_bucket,
           s.trip_bucket,
           avg_distance,
           rev.avg_duration,
           rev.avg_revenue,
           rev.trips_total_gmv,
           rev.trip_count,
           rev.days_since_least_recent_trip,
           rev.days_since_most_recent_trip,
           rev.exp_weight_90,
           rev.exp_weight_60,
           rev.exp_weight_30,
           rev.exp_weight_10,
           rev.exp_weight_5
      FROM base b
 LEFT JOIN preferred_cct c
        ON c.customer_id = b.customer_id
 LEFT JOIN ride_usage r
        ON r.customer_id = b.customer_id
 LEFT JOIN ride_usage_classification u
        ON u.customer_id = b.customer_id
 LEFT JOIN time_of_day_commuter t
        ON t.customer_id = b.customer_id
 LEFT JOIN preferred_payment_method p
        ON p.customer_id = b.customer_id
 LEFT JOIN parent_status m
        ON m.customer_id = b.customer_id
 LEFT JOIN promo_behaviour f
        ON f.customer_id = b.customer_id
 LEFT JOIN spend_bucket s
        ON s.customer_id = b.customer_id
LEFT JOIN avg_revenue_data rev
        ON rev.customer_id = b.customer_id