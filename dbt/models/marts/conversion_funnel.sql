-- Business metric: conversion funnel
SELECT
    COUNT(CASE WHEN event_type = 'page_view'       THEN 1 END) AS page_views,
    COUNT(CASE WHEN event_type = 'product_view'    THEN 1 END) AS product_views,
    COUNT(CASE WHEN event_type = 'add_to_cart'     THEN 1 END) AS add_to_cart,
    COUNT(CASE WHEN event_type = 'checkout_start'  THEN 1 END) AS checkout_starts,
    COUNT(CASE WHEN event_type = 'purchase'        THEN 1 END) AS purchases,
    ROUND(
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END)::numeric /
        NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0) * 100, 2
    ) AS view_to_cart_rate,
    ROUND(
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)::numeric /
        NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0) * 100, 2
    ) AS cart_to_purchase_rate
FROM {{ ref('stg_events') }}