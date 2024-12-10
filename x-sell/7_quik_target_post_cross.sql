,post_cross_quik_behavior AS (
    SELECT
    date_diff(
        'day',
        CAST('{anchor_date}' AS date),
        CAST(order_received_timestamp AS date)
    ) AS days_since_first_quik_order,
     a.customer_id, a.order_id,
     a.promo_amount, a.basket_amount, a.net_revenue,
     a.net_basket_amount_with_tax, a.delivery_fee,
     a.gmv, a.careem_plus_discount,
     a.promo_code, a.promo_type, a.promo_applied_value, 
     a.order_received_timestamp, a.merchant_name, a.brand_name,
    --  c.name AS cuisine, c.id as cuisine_merchant_priority,
     DENSE_RANK() over (PARTITION BY a.customer_id order by order_received_timestamp) as order_rank
      FROM now_prod_dwh.orders a
      INNER JOIN customer_base cb on a.customer_id = cb.customer_id
     WHERE
     order_type = 'shops'
     AND LOWER(merchant_name) LIKE '%quik%'
    --  AND json_extract_scalar(a.summary, '$.upc') IS NOT NULL
       AND DATE(a.day) >= DATE('{anchor_date}')
       AND DATE(a.day) < DATE('{target_end_date}')
    --    AND c.type = 'cuisine'
    --    AND lower(b.status) = 'active'
    --    AND order_status='delivered'
)
SELECT 
* from post_cross_quik_behavior