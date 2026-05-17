SELECT
    product_id,
    product_name,
    category,
    unit_price::FLOAT AS unit_price,
    supplier
FROM {{ source('bronze_raw', 'raw_products') }}
