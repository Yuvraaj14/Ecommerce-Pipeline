-- Staging: clean raw events
SELECT
    event_id,
    event_type,
    user_id,
    session_id,
    product_id,
    product_name,
    product_category,
    price,
    quantity,
    revenue,
    timestamp::timestamptz AS event_timestamp,
    country,
    device,
    is_new_user,
    processed_at
FROM raw_events
WHERE event_id IS NOT NULL