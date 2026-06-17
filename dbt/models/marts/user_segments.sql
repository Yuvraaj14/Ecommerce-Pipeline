-- Business metric: user segmentation
SELECT
    user_id,
    COUNT(DISTINCT session_id)                              AS total_sessions,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)    AS total_purchases,
    SUM(revenue)                                            AS total_spent,
    AVG(revenue)                                            AS avg_order_value,
    MIN(timestamp::timestamptz)                             AS first_seen,
    MAX(timestamp::timestamptz)                             AS last_seen,
    CASE
        WHEN SUM(revenue) > 100000 THEN 'VIP'
        WHEN SUM(revenue) > 50000  THEN 'High Value'
        WHEN SUM(revenue) > 10000  THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM {{ ref('stg_events') }}
GROUP BY user_id
ORDER BY total_spent DESC