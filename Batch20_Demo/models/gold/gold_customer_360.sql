SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city,
    c.state,
    c.customer_tier,
    COUNT(DISTINCT t.order_id) AS total_orders,
    SUM(t.total_amount) AS lifetime_revenue,
    AVG(t.total_amount) AS avg_order_value,
    MIN(t.order_date) AS first_order_date,
    MAX(t.order_date) AS last_order_date,
    DATEDIFF('day', MAX(t.order_date), CURRENT_DATE()) AS days_since_last_order,
    CASE
        WHEN SUM(t.total_amount) >= 5000 THEN 'Platinum'
        WHEN SUM(t.total_amount) >= 2000 THEN 'Gold'
        WHEN SUM(t.total_amount) >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS loyalty_segment
FROM {{ ref('dim_customers') }} c
LEFT JOIN {{ ref('fact_transactions') }} t ON c.customer_id = t.customer_id
WHERE t.order_status = 'completed'
GROUP BY 1, 2, 3, 4, 5, 6
