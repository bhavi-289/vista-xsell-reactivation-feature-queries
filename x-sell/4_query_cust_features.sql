,base_data AS (
    SELECT CAST(customer_id AS BIGINT) AS customer_id
      FROM dev_bi.LS_final_table
     WHERE 1=1
       AND CAST(customer_id AS BIGINT) in (select customer_id from customer_base)
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
  GROUP BY 1
)
,gender_data AS (
    SELECT CAST(customer_id AS BIGINT) AS customer_id,
           IF(trim(BOTH ' ' FROM LOWER(sex_prediction)) IN ('female', 'male'), trim(BOTH ' ' FROM LOWER(sex_prediction)), NULL) AS gender,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY day DESC) AS rn_gender
      FROM dev_bi.LS_final_table
     WHERE 1=1
     AND CAST(customer_id AS BIGINT) in (select customer_id from customer_base)
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
       AND trim(BOTH ' ' FROM LOWER(sex_prediction)) IN ('female', 'male')
)
,tourist_data AS (
    SELECT CAST(customer_id AS BIGINT) AS customer_id,
           IF(trim(BOTH ' ' FROM LOWER(tourist_flag)) IN ('tourist'), 1, NULL) AS is_tourist,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY day DESC) AS rn_tourist
      FROM dev_bi.LS_final_table
     WHERE 1=1
     AND CAST(customer_id AS BIGINT) in (select customer_id from customer_base)
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
       AND trim(BOTH ' ' FROM LOWER(tourist_flag)) IN ('tourist')
)
,tourist_country_data AS (
    SELECT CAST(customer_id AS BIGINT) AS customer_id,
           trim(BOTH ' ' FROM LOWER(tourist_country_simcard)) AS sim_country,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY day DESC) AS rn_country
      FROM dev_bi.LS_final_table
     WHERE 1=1
     AND CAST(customer_id AS BIGINT) in (select customer_id from customer_base)
       AND day >= DATE('{source_start_date}')
       AND day < DATE('{anchor_date}')
       AND tourist_country_simcard IS NOT NULL
)
    SELECT b.customer_id,
           g.gender,
           t.is_tourist,
           c.sim_country
      FROM base_data b
 LEFT JOIN (SELECT customer_id, gender FROM gender_data WHERE rn_gender = 1) g
        ON b.customer_id = g.customer_id
 LEFT JOIN (SELECT customer_id, is_tourist FROM tourist_data WHERE rn_tourist = 1) t
        ON b.customer_id = t.customer_id
 LEFT JOIN (SELECT customer_id, sim_country FROM tourist_country_data WHERE rn_country = 1) c
        ON b.customer_id = c.customer_id

