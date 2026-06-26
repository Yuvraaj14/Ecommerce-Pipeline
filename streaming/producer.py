"""
E-Commerce Event Producer
Simulates real-time e-commerce events → Kafka topic
Author: Yuvraaj M N
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

# ============================================
# CONFIG
# ============================================
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
TOPIC_NAME = 'ecommerce-events'
EVENTS_PER_SECOND = 10

# ============================================
# DATA GENERATORS
# ============================================
PRODUCTS = [
    {"id": "P001", "name": "iPhone 15", "category": "electronics", "price": 79999},
    {"id": "P002", "name": "Samsung TV 55\"", "category": "electronics", "price": 54999},
    {"id": "P003", "name": "Nike Air Max", "category": "footwear", "price": 8999},
    {"id": "P004", "name": "Levi's Jeans", "category": "clothing", "price": 2999},
    {"id": "P005", "name": "Python Programming", "category": "books", "price": 599},
    {"id": "P006", "name": "MacBook Pro", "category": "electronics", "price": 149999},
    {"id": "P007", "name": "Wireless Headphones", "category": "electronics", "price": 3999},
    {"id": "P008", "name": "Running Shoes", "category": "footwear", "price": 4999},
    {"id": "P009", "name": "Formal Shirt", "category": "clothing", "price": 1499},
    {"id": "P010", "name": "Data Science Book", "category": "books", "price": 799},
]

EVENT_TYPES = [
    "page_view",       # 35%
    "product_view",    # 30%
    "add_to_cart",     # 15%
    "remove_from_cart",# 5%
    "checkout_start",  # 8%
    "purchase",        # 7%
]

EVENT_WEIGHTS = [35, 30, 15, 5, 8, 7]

COUNTRIES = ["IN", "US", "UK", "DE", "SG", "AU", "CA"]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [55, 35, 10]


def generate_event(user_pool: list) -> dict:
    """Generate a realistic e-commerce event"""

    product = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    user_id = random.choice(user_pool)

    # Purchase quantity logic
    quantity = 1
    if event_type in ["add_to_cart", "purchase"]:
        quantity = random.choices([1, 2, 3, 4, 5], weights=[60, 20, 10, 6, 4])[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": f"sess_{str(uuid.uuid4())[:8]}",
        "product_id": product["id"],
        "product_name": product["name"],
        "product_category": product["category"],
        "price": product["price"],
        "quantity": quantity,
        "revenue": product["price"] * quantity if event_type == "purchase" else 0,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "country": random.choice(COUNTRIES),
        "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        "is_new_user": random.random() < 0.3,
    }

    return event


def create_producer() -> KafkaProducer:
    """Create Kafka producer with JSON serialization"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(3, 5, 0),

        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,

        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    logger.info(f"✅ Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    return producer


def run_producer(events_per_second: int = EVENTS_PER_SECOND,
                 duration_seconds: int = None):
    """
    Run the event producer
    duration_seconds=None → runs forever
    """

    producer = create_producer()

    # Simulate 500 unique users
    user_pool = [f"user_{str(uuid.uuid4())[:8]}" for _ in range(500)]

    total_events = 0
    start_time = time.time()
    interval = 1.0 / events_per_second

    print(f"\n{'='*60}")
    print(f"🛒 E-COMMERCE EVENT PRODUCER STARTED")
    print(f"{'='*60}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Rate:  {events_per_second} events/second")
    print(f"Users: {len(user_pool)} simulated users")
    print(f"{'='*60}\n")

    try:
        while True:
            event = generate_event(user_pool)

            # Use user_id as key for partition affinity
            producer.send(
                TOPIC_NAME,
                key=event["user_id"],
                value=event
            )

            total_events += 1

            # Log every 100 events
            if total_events % 100 == 0:
                elapsed = time.time() - start_time
                rate = total_events / elapsed
                logger.info(
                    f"📤 Sent {total_events} events | "
                    f"Rate: {rate:.1f}/s | "
                    f"Latest: {event['event_type']} - "
                    f"{event['product_name']} (₹{event['price']})"
                )

            # Duration check
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                logger.info(f"✅ Duration reached: {duration_seconds}s")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(f"\n⛔ Producer stopped. Total events sent: {total_events}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    run_producer(events_per_second=10)