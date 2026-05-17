SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date::DATE AS order_date,
    order_status,
    payment_method
FROM {{ source('bronze_raw', 'raw_orders') }}
