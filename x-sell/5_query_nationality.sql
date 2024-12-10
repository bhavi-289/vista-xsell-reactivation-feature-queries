select
CAST(n.customer_id AS BIGINT) AS customer_id,
account_predicted_nationality as nationality,
CAST(account_predicted_nationality_score as double) as confidence,
account_region_based_on_nationality as region,
account_subregion_based_on_nationality as sub_region,
platform_is_loyal_customer,
account_is_uae_resident,
platform_services_used_by_name_count_60d
from shared.customer_profile_dim n
JOIN customer_base v
ON CAST(n.customer_id AS BIGINT) = v.customer_id
     --WHERE CAST(confidence AS DOUBLE) > 0.8