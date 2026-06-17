"""
Write Kafka events directly to PostgreSQL (Supabase)
Author: Yuvraaj M N
"""

import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = psycopg2.connect(
    host=os.getenv("SUPABASE_HOST"),
    port=os.getenv("SUPABASE_PORT", 5432),
    database=os.getenv("SUPABASE_DB", "postgres"),
    user=os.getenv("SUPABASE_USER", "postgres"),
    password=os.getenv("SUPABASE_PASSWORD")
)
conn.autocommit = True
cursor = conn.cursor()

consumer = KafkaConsumer(
    'ecommerce-events',
    bootstrap_servers='localhost:9092',
    group_id='postgres-writer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest'
)

print("📥 Writing events to Supabase PostgreSQL...")
count = 0

for message in consumer:
    event = message.value
    try:
        cursor.execute("""
            INSERT INTO raw_events (
                event_id, event_type, user_id, session_id,
                product_id, product_name, product_category,
                price, quantity, revenue, timestamp,
                country, device, is_new_user, processed_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT (event_id) DO NOTHING
        """, (
            event['event_id'], event['event_type'], event['user_id'],
            event['session_id'], event['product_id'], event['product_name'],
            event['product_category'], event['price'], event['quantity'],
            event['revenue'], event['timestamp'], event['country'],
            event['device'], event['is_new_user'],
            datetime.utcnow().isoformat()
        ))
        count += 1
        if count % 100 == 0:
            logger.info(f"✅ Inserted {count} events to Supabase")
    except Exception as e:
        logger.warning(f"Insert error: {e}")