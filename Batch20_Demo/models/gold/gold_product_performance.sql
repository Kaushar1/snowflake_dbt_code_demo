SELECT
    product_category,
    product_name,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) AS return_count,
    ROUND(SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM {{ ref('fact_transactions') }}
GROUP BY 1, 2
ORDER BY total_revenue DESC
