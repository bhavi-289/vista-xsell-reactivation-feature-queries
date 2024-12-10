,bookings AS (
    SELECT CAST(b.customer_id AS BIGINT) AS customer_id,
           CAST(b.dropoff_lat AS DOUBLE) AS drop_off_latitude,
           CAST(b.dropoff_long AS DOUBLE) AS drop_off_longitude,
           CAST(b.booking_creation_date AS TIMESTAMP) AS pick_up_time,
           CAST(b.booking_id AS BIGINT) AS booking_id
      FROM prod_dwh.booking b
     WHERE 1=1
       AND b.day >= DATE('{source_start_date}')
       AND b.day < DATE('{anchor_date}')

       AND CAST(b.customer_id AS BIGINT) in (select customer_id from customer_base)
       
       AND b.service_area_id = 1
       AND b.booking_status = 6
       AND b.is_trip_ended = TRUE
       AND b.is_cancelled_customer = FALSE
       AND b.is_cancelled_captain = FALSE
       AND b.booking_id IS NOT NULL
       AND b.is_assigned = TRUE
       AND b.captain_id IS NOT NULL
       AND LOWER(b.business_type) IN ('jv', 'hala', 'ride-hailing', 'ridehailing', 'ride hailing')
  GROUP BY 1,2,3,4,5
)
,saved_locations AS(
    SELECT bl.customer_id,
           bl.location_id,
           ROW_NUMBER() OVER(PARTITION BY bl.customer_id, bl.location_id ORDER BY bl.last_updated DESC) AS r,
           bl.created_at,
           bl.last_updated,
           bli.saved_name,
           bli.address_type,
           bli.building_type,
           bli.place_name,
           blc.saved_location_latitude,
           blc.saved_location_longitude,
           blc.saved_location_point
      FROM (
               SELECT CAST(user_id AS BIGINT) AS customer_id,
                      CAST(id AS BIGINT) AS location_id,
                      CAST(created_at AS TIMESTAMP) AS created_at,
                      CAST(last_updated AS TIMESTAMP) AS last_updated,
                      ROW_NUMBER() OVER(PARTITION BY user_id, id ORDER BY last_updated DESC) AS r
                 FROM prod_dwh.bookmark_user_location
                 where
                 CAST(user_id AS BIGINT) in (select customer_id from customer_base)
           ) bl
INNER JOIN (
               SELECT CAST(location_bookmark_id AS BIGINT) AS location_id,
                      TO_SPHERICAL_GEOGRAPHY(ST_GEOMFROMBINARY(FROM_HEX(REPLACE(lng_lat, '20E61000')))) AS saved_location_point,
                      CAST(ST_Y(ST_GEOMFROMBINARY(FROM_HEX(REPLACE(lng_lat, '20E61000')))) AS double) AS saved_location_latitude,
                      CAST(ST_X(ST_GEOMFROMBINARY(FROM_HEX(REPLACE(lng_lat, '20E61000')))) AS double) AS saved_location_longitude,
                      ROW_NUMBER() OVER(PARTITION BY location_bookmark_id ORDER BY last_updated DESC) AS r
                 FROM prod_dwh.bookmark_user_location_coordinates
           ) blc
        ON bl.location_id = blc.location_id
       AND bl.r = 1
       AND blc.r = 1
INNER JOIN (
               SELECT CAST(location_bookmark_id AS BIGINT) AS location_id,
                      CAST(saved_name AS varchar) AS saved_name,
                      CAST(address_type AS varchar) AS address_type,
                      CAST(building_type AS varchar) AS building_type,
                      CAST(place_name AS varchar) AS place_name,
                      ROW_NUMBER() OVER(PARTITION BY location_bookmark_id ORDER BY last_updated DESC) AS r
                 FROM prod_dwh.bookmark_address_detail_component
           ) bli
        ON bl.location_id = bli.location_id
       AND bl.r = 1
       AND bli.r = 1
)
,enriched_bookings AS (
    SELECT b.*,
           CASE WHEN sl.location_id IS NOT NULL THEN 1 ELSE 0 END AS is_sl,
           row_number() over(partition by b.customer_id, b.booking_id ORDER BY sl.created_at DESC, sl.last_updated DESC) AS sl_r,
           sl.location_id AS saved_location_id,
           sl.created_at AS sl_created_at,
           LOWER(sl.saved_name) AS sl_name,
           LOWER(sl.address_type) AS sl_address_type,
           LOWER(sl.building_type) AS sl_building_type,
           LOWER(sl.place_name) AS sl_place_name,
           sl.saved_location_latitude AS sl_lat,
           sl.saved_location_longitude AS sl_lon
      FROM bookings b
 LEFT JOIN saved_locations sl
        ON b.customer_id = sl.customer_id
       AND sl.r = 1
       AND 1000.0 * 2.0 * 6371.009 *
           asin(sqrt(pow(sin((radians(sl.saved_location_latitude) - radians(b.drop_off_latitude)) / 2.0), 2) +
                cos(radians(b.drop_off_latitude)) * cos(radians(sl.saved_location_latitude)) *
                pow(sin((radians(sl.saved_location_longitude) - radians(b.drop_off_longitude)) / 2.0), 2))) < 100
       AND b.pick_up_time > sl.created_at
)
,location_counts AS (
    SELECT b.customer_id,
           CASE
               WHEN sl_name LIKE '%work%' OR sl_name LIKE '%office%' OR sl_name LIKE '%pwc%' OR sl_name LIKE '%hq%' OR sl_name LIKE '%mckinsey%' OR sl_building_type LIKE '%office%' THEN 'work'
               WHEN sl_name LIKE '%gym%' OR sl_name LIKE '%yoga%' OR sl_name LIKE '%tennis%' OR sl_name LIKE '%football%' OR sl_name LIKE '%training%' OR sl_name LIKE '%swimming%' OR sl_name LIKE '%golf%' OR sl_name LIKE '%pilates%' OR sl_name LIKE '%basketball%' THEN 'sport'
               WHEN sl_name LIKE '%school%' OR sl_name LIKE '%university%' OR sl_name LIKE '%uni%' OR sl_name LIKE '%college%' THEN 'school'
               WHEN sl_name LIKE '%airport%' OR sl_name LIKE '%terminal%' OR sl_name LIKE '%dxb%' OR sl_name LIKE '%departures%' OR sl_name LIKE '%arrivals%' OR sl_name LIKE '%t1%' OR sl_name LIKE '%t2%' OR sl_name LIKE '%t3%' THEN 'airport'
               WHEN sl_name LIKE '%mall%' OR sl_name LIKE '%store%' OR sl_name LIKE '%emirates%' OR sl_name LIKE '%ikea%' OR sl_name LIKE '%shop%' THEN 'mall'
               WHEN sl_name LIKE '%hotel%' OR sl_name LIKE '%hôtel%' OR sl_name LIKE '%sheraton%' OR sl_name LIKE '%holiday inn%' OR sl_name LIKE '%premier inn%' OR sl_name LIKE '%hyatt%' OR sl_name LIKE '%radisson%' OR sl_name LIKE '%movenpick%' OR sl_name LIKE '%ibis%' OR sl_name LIKE '%novotel%' OR sl_name LIKE '%hilton%' OR sl_name LIKE '%marriott%' OR sl_name LIKE '%otel%' THEN 'hotel'
               WHEN sl_name LIKE '%hospital%' OR sl_name LIKE '%clinic%' OR sl_name LIKE '%dentist%' OR sl_name LIKE '%nursery%' THEN 'hospital'
               ELSE sl_name
           END AS classified_location,
           COUNT(*) AS location_count
      FROM enriched_bookings b
     WHERE ((is_sl = 1 AND sl_r = 1) OR is_sl = 0)
           AND sl_name IS NOT NULL
           AND sl_name NOT LIKE '%home%'
           AND sl_name NOT LIKE '%room%'
           AND sl_name NOT LIKE '%house%'
           AND sl_name NOT LIKE '%villa%'
           AND sl_name NOT LIKE '%apartment%'
           AND sl_address_type NOT LIKE '%apartment%'
           AND sl_building_type NOT LIKE '%apartment%'
           AND sl_building_type NOT LIKE '%villa%'
           AND sl_place_name NOT LIKE '%villa%'
  GROUP BY 1,2
)
,location_percentage AS (
    SELECT customer_id,
           classified_location,
           location_count,
           (location_count * 1.0 / SUM(location_count) OVER (PARTITION BY customer_id)) * 100 AS percentage
      FROM location_counts
)
,ranked_locations AS (
    SELECT customer_id,
           classified_location,
           location_count,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY percentage DESC) AS rn
      FROM location_percentage
)
    SELECT customer_id,
           classified_location AS commute_type,
           location_count
      FROM ranked_locations
     WHERE rn = 1
       AND location_count >= {saved_loc_min_trip_cnt}
