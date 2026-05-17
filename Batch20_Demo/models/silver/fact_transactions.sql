{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT
    o.order_id,
    o.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.product_id,
    p.product_name,
    p.category AS product_category,
    o.quantity,
    p.unit_price,
    o.quantity * p.unit_price AS total_amount,
    o.order_date,
    o.order_status,
    o.payment_method,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id

{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
