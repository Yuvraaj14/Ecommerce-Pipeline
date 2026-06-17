-- Business metric: top products by revenue
SELECT
    product_name,
    product_category,
    COUNT(DISTINCT user_id)     AS unique_buyers,
    COUNT(event_id)             AS total_orders,
    SUM(revenue)                AS total_revenue,
    AVG(price)                  AS avg_price,
    SUM(quantity)               AS total_units_sold,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS revenue_rank
FROM {{ ref('stg_purchases') }}
GROUP BY product_name, product_category
ORDER BY total_revenue DESC
LIMIT 10