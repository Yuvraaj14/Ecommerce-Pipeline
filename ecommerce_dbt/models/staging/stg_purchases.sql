-- Staging: purchases only
SELECT
    event_id,
    user_id,
    product_id,
    product_name,
    product_category,
    price,
    quantity,
    revenue,
    timestamp::timestamptz AS purchase_timestamp,
    country,
    device
FROM raw_events
WHERE event_type = 'purchase'
  AND revenue > 0