SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at
FROM {{ source('bronze_raw', 'raw_customers') }}
