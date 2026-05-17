SELECT
    DATE_TRUNC('month', order_date) AS revenue_month,
    product_category,
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS monthly_revenue,
    SUM(quantity) AS total_units,
    AVG(total_amount) AS avg_order_value,
    SUM(CASE WHEN order_status = 'completed' THEN total_amount ELSE 0 END) AS completed_revenue,
    SUM(CASE WHEN order_status = 'returned' THEN total_amount ELSE 0 END) AS returned_revenue
FROM {{ ref('fact_transactions') }}
GROUP BY 1, 2, 3
ORDER BY revenue_month DESC, monthly_revenue DESC
