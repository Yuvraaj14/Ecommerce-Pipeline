"""
E-Commerce Event Consumer
Reads from Kafka → forwards to Spark / PostgreSQL / Elasticsearch
Author: Yuvraaj M N
"""

import json
import logging
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
TOPIC_NAME = 'ecommerce-events'
GROUP_ID = 'ecommerce-pipeline-group'


def create_consumer() -> KafkaConsumer:
    """Create Kafka consumer"""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        max_poll_records=100,
    )
    logger.info(f"✅ Consumer connected | Topic: {TOPIC_NAME} | Group: {GROUP_ID}")
    return consumer


def process_event(event: dict) -> dict:
    """Process and enrich incoming event"""

    # Add processing timestamp
    event['processed_at'] = datetime.utcnow().isoformat() + "Z"

    # Add derived fields
    event['is_purchase'] = event['event_type'] == 'purchase'
    event['is_cart_event'] = event['event_type'] in ['add_to_cart', 'remove_from_cart']

    return event


def run_consumer(max_messages: int = None):
    """Run the event consumer"""

    consumer = create_consumer()

    total = 0
    event_counts = {}

    print(f"\n{'='*60}")
    print(f"📥 E-COMMERCE EVENT CONSUMER STARTED")
    print(f"{'='*60}")

    try:
        for message in consumer:
            event = message.value
            event = process_event(event)

            # Track counts
            event_type = event.get('event_type', 'unknown')
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
            total += 1

            # Log every 50 messages
            if total % 50 == 0:
                logger.info(f"📊 Consumed {total} events")
                logger.info(f"   Breakdown: {event_counts}")

            if max_messages and total >= max_messages:
                logger.info(f"✅ Reached max messages: {max_messages}")
                break

    except KeyboardInterrupt:
        logger.info(f"\n⛔ Consumer stopped. Total: {total}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()