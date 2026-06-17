-- Business metric: revenue by product category
SELECT
    product_category,
    COUNT(DISTINCT user_id)     AS unique_buyers,
    COUNT(event_id)             AS total_orders,
    SUM(revenue)                AS total_revenue,
    AVG(revenue)                AS avg_order_value,
    SUM(quantity)               AS total_units_sold
FROM {{ ref('stg_purchases') }}
GROUP BY product_category
ORDER BY total_revenue DESC