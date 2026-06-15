# Run this once to create tables
# spark/setup_db.py

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("SUPABASE_HOST", "localhost"),
    port=os.getenv("SUPABASE_PORT", 5432),
    database=os.getenv("SUPABASE_DB", "postgres"),
    user=os.getenv("SUPABASE_USER", "postgres"),
    password=os.getenv("SUPABASE_PASSWORD", "postgres")
)

cursor = conn.cursor()

# Raw events table
cursor.execute("""
CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR,
    user_id VARCHAR,
    session_id VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    price FLOAT,
    quantity INT,
    revenue FLOAT,
    timestamp TIMESTAMPTZ,
    country VARCHAR,
    device VARCHAR,
    is_new_user BOOLEAN,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# Aggregated metrics table
cursor.execute("""
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    batch_id INT,
    metric_name VARCHAR,
    metric_value FLOAT,
    batch_time TIMESTAMPTZ DEFAULT NOW()
);
""")

# Revenue by hour
cursor.execute("""
CREATE TABLE IF NOT EXISTS revenue_hourly (
    id SERIAL PRIMARY KEY,
    hour TIMESTAMPTZ,
    total_revenue FLOAT,
    total_orders INT,
    avg_order_value FLOAT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

conn.commit()
cursor.close()
conn.close()
print("✅ Tables created successfully!")