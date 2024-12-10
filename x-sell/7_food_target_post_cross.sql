,post_cross_food_behavior AS (
    SELECT
    date_diff(
        'day',
        CAST('{anchor_date}' AS date),
        CAST(order_received_timestamp AS date)
    ) AS days_since_first_food_order,
     a.customer_id, a.order_id,
     a.promo_amount, a.basket_amount, a.net_revenue,
     a.net_basket_amount_with_tax, a.delivery_fee,
     a.gmv, a.careem_plus_discount,
     a.promo_code, a.promo_type, a.promo_applied_value, 
     a.order_received_timestamp, a.merchant_name, a.brand_name,
     c.name AS cuisine, c.id as cuisine_merchant_priority,
     DENSE_RANK() over (PARTITION BY a.customer_id order by order_received_timestamp) as order_rank
      FROM now_prod_dwh.orders a
INNER JOIN customer_base cb on a.customer_id = cb.customer_id
INNER JOIN careem_now.merchants__merchant_has_tags b ON a.merchant_id = b.merchant_id
INNER JOIN careem_now.content__tags c ON b.tag_id = c.id
     WHERE order_type = 'food'
       AND DATE(a.day) >= DATE('{anchor_date}')
       AND DATE(a.day) < DATE('{target_end_date}')
       AND c.type = 'cuisine'
       AND lower(b.status) = 'active'
       AND order_status='delivered'
)
SELECT 
*,
case
when lower(cuisine) in (
    'asian',
    'rice',
    'sushi',
    'noodles & ramen',
    'poké',
    'pan asian',
    'momos',
    'dumplings',

    'filipino',
    'afghan',
    'singaporean',
    'japanese',
    'vietnamese',
    'thai',
    'indonesian',
    'malaysian',
    'korean',
    'chinese',
    'indo-chinese',
    'nepali',
    'pan-asian'
) then 'Asian'
when lower(cuisine) in (
    'middle eastern',
    'kebab',
    'doner',
    'shawerma',

    'syrian',
    'iranian',
    'turkish',
    'kuwaiti',
    'palestinian',
    'lebanese',
    'emirati',
    'iraqi',
    'saudi',
    'yemeni',
    'egyptian',
    'jordanian',
    'arabic',
    'mandi',
    'manakish',
    'saj',
    'shawarma',
--        'kunafa',
    'foul',
    'mansaf',
    'vine leaves',
    'falafel'
) then 'Middle Eastern & Arabic'
when lower(cuisine) in (
    'european',
    
    'french',
    'scandinavian',
    'british',
    'german',
    'continental'
) then 'European'
when lower(cuisine) in (
    'american',
    'tex-mex',
    'canadian',
    'bbq & grill'
--      'burger',
--      'hot dog',
--      'fried chicken',
--      'wings'
) then 'American'
when lower(cuisine) in (
    'african', 
    
    'afro-portuguese',
    'south african',
    'moroccan',
    'algerian'
) then 'African'
when lower(cuisine) in (
    'sri lankan',
    
    'indian',
    'pakistani',
    'hyderabadi',
    'bengali',
    'gujarati',
    'kerala',
    'north indian',
    'south indian',
    'biryani',
    'tandoor',
    'desi',
    'chettinad'
) then 'South Asian'
when lower(cuisine) in (
    'brazillian',
    
    'brazilian',
    'mexican',
    'burritos',
    'tacos',
    'caribbean'
) then 'Latin American'
when lower(cuisine) in (
    'greek',
    'pasta',
    
    'italian',
    'portuguese',
    'spanish',
    'mediterranean'
) then 'Mediterranean'
when lower(cuisine) in (
    'healthy',
    'smoothies ', --- yes, it's written with the whitespace in the end in dwh source
    'healthy juice',
    'soup',
    
    'healthy food',
    'gluten-free',
    'healthy juice',
    'acai',
    'smoothies',
    'salads',
    'bowls'
) then 'Healthy & Specialty'
when lower(cuisine) in (
    'fries',
    'sandwiches',
    'burger',
    'hot dog',
    'fried chicken',
    'wings',
    
    'fast food',
    'pizza',
    'sandwiches & wraps',
    'wraps',
    'rolls',
    'grills'
) then 'Fast Food'
when lower(cuisine) in (
    'bakery & confectionery',
    'bakery',
    'pie',
    'kunafa',

    'desserts',
    'cakes',
    'cookies',
    'pastries',
    'sweets',
    'ice cream',
    'crepes',
    'waffles',
    'frozen yogurt',
--        'kunafa',
    'donuts',
    'chocolates',
    'luqaimat',
    'pies'
) then 'Desserts & Sweets'
when lower(cuisine) in (
    'coffee',
    'hot beverages',
    'beverages',
    'soft drinks',
    'shakes',
    'juices',
    'cocktails',
    'espresso',
    'tea'
) then 'Beverages & Coffee'
when lower(cuisine) in (
    'cafe',
    'cafeteria',
    'street food',
    'snacks',
    'kumpir'
--        'saj',
--        'shawarma'
) then 'Cafeteria & Street Food'
when lower(cuisine) in (
    'seafood',
    'fish',
    'shrimps'
) then 'Seafood'
when lower(cuisine) in (
    'uzbekistan',
    
    'russian',
    'uzbek'
) then 'Russian'
else 'Other'
end as HLCC_ORIGINAL
 from post_cross_food_behavior