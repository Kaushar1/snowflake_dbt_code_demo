{% snapshot scd_customers %}

{{
    config(
        target_database='DEMO_DB',
        target_schema='SILVER',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    created_at,
    updated_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
