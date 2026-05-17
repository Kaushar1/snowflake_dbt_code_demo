SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    created_at,
    updated_at,
    CASE
        WHEN state IN ('NY', 'CA', 'TX', 'IL') THEN 'Tier 1'
        WHEN state IN ('AZ', 'FL', 'PA', 'OH') THEN 'Tier 2'
        ELSE 'Tier 3'
    END AS customer_tier,
    DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) AS days_since_signup
FROM {{ ref('stg_customers') }}
