"""
PySpark Streaming Job
Reads from Kafka → processes → writes to PostgreSQL + Redis + Elasticsearch
Author: Yuvraaj M N
"""

import json
import redis
import psycopg2
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum,
    avg, max as spark_max, when, current_timestamp,
    to_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, BooleanType, LongType
)
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONFIG
# ============================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"
CHECKPOINT_DIR = os.path.join(os.getcwd(), "spark", "checkpoints").replace("\\", "/")
BATCH_INTERVAL = "30 seconds"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_TTL = 30  # seconds

ES_HOST = "http://localhost:9200"
ES_INDEX = "ecommerce-events"

DB_CONFIG = {
    "host": os.getenv("SUPABASE_HOST", "localhost"),
    "port": os.getenv("SUPABASE_PORT", "5432"),
    "database": os.getenv("SUPABASE_DB", "ecommerce"),
    "user": os.getenv("SUPABASE_USER", "postgres"),
    "password": os.getenv("SUPABASE_PASSWORD", "postgres")
}

# ============================================
# EVENT SCHEMA
# ============================================
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("product_category", StringType()),
    StructField("price", FloatType()),
    StructField("quantity", IntegerType()),
    StructField("revenue", FloatType()),
    StructField("timestamp", StringType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
    StructField("is_new_user", BooleanType()),
    StructField("is_purchase", BooleanType()),
    StructField("is_cart_event", BooleanType()),
    StructField("processed_at", StringType()),
])

# ============================================
# SPARK SESSION
# ============================================
def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka + ES packages"""
    spark = SparkSession.builder \
        .appName("EcommerceStreamingPipeline") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.hadoop.hadoop.home.dir", "C:/hadoop") \
        .config("spark.sql.streaming.checkpointLocation",
                os.path.join(os.getcwd(), "spark", "checkpoints").replace("\\", "/")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session created")
    return spark


# ============================================
# REDIS WRITER
# ============================================
class RedisWriter:
    """Writes aggregated metrics to Redis hot cache"""

    def __init__(self):
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )

    def write_metrics(self, metrics: dict):
        """Write metrics to Redis with TTL"""
        try:
            for key, value in metrics.items():
                self.client.setex(
                    f"dashboard:{key}",
                    REDIS_TTL,
                    json.dumps(value)
                )
            logger.info(f"⚡ Redis updated: {list(metrics.keys())}")
        except Exception as e:
            logger.warning(f"Redis write failed: {e}")

    def get_metrics(self, key: str):
        """Get metric from Redis"""
        try:
            val = self.client.get(f"dashboard:{key}")
            return json.loads(val) if val else None
        except Exception:
            return None


# ============================================
# ELASTICSEARCH WRITER
# ============================================
class ESWriter:
    """Indexes events to Elasticsearch for log search"""

    def __init__(self):
        self.client = Elasticsearch(ES_HOST)
        self._ensure_index()

    def _ensure_index(self):
        """Create index with mapping if not exists"""
        if not self.client.indices.exists(index=ES_INDEX):
            mapping = {
                "mappings": {
                    "properties": {
                        "event_id": {"type": "keyword"},
                        "event_type": {"type": "keyword"},
                        "user_id": {"type": "keyword"},
                        "product_name": {"type": "text"},
                        "product_category": {"type": "keyword"},
                        "price": {"type": "float"},
                        "revenue": {"type": "float"},
                        "country": {"type": "keyword"},
                        "device": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                    }
                }
            }
            self.client.indices.create(index=ES_INDEX, body=mapping)
            logger.info(f"✅ ES index created: {ES_INDEX}")

    def index_batch(self, events: list):
        """Bulk index events to ES"""
        if not events:
            return
        try:
            from elasticsearch.helpers import bulk
            actions = [
                {"_index": ES_INDEX, "_id": e["event_id"], "_source": e}
                for e in events
            ]
            bulk(self.client, actions)
            logger.info(f"📝 ES indexed {len(events)} events")
        except Exception as e:
            logger.warning(f"ES write failed: {e}")


# ============================================
# POSTGRESQL WRITER
# ============================================
def write_to_postgres(df, table_name: str):
    """Write Spark DataFrame to PostgreSQL"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}") \
            .option("dbtable", table_name) \
            .option("user", DB_CONFIG['user']) \
            .option("password", DB_CONFIG['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logger.info(f"✅ PostgreSQL written: {table_name}")
    except Exception as e:
        logger.warning(f"PostgreSQL write failed: {e}")


# ============================================
# BATCH PROCESSOR
# ============================================
def process_batch(batch_df, batch_id: int):
    """
    Process each micro-batch:
    1. Compute real-time aggregations
    2. Write raw events to ES
    3. Cache aggregations in Redis
    4. Write aggregations to PostgreSQL
    """

    if batch_df.isEmpty():
        return

    count_val = batch_df.count()
    logger.info(f"\n{'='*50}")
    logger.info(f"⚡ Batch {batch_id}: {count_val} events")
    logger.info(f"{'='*50}")

    redis_writer = RedisWriter()
    es_writer = ESWriter()
    batch_time = datetime.utcnow().isoformat()

    # ─── 1. Raw events to Elasticsearch ───
    events_list = [row.asDict() for row in batch_df.collect()]
    es_writer.index_batch(events_list)

    # ─── 2. Revenue metrics ───
    revenue_df = batch_df.filter(col("event_type") == "purchase") \
        .agg(
            spark_sum("revenue").alias("total_revenue"),
            count("event_id").alias("total_orders"),
            avg("revenue").alias("avg_order_value"),
            spark_max("revenue").alias("max_order_value")
        )

    revenue_row = revenue_df.first()
    revenue_metrics = {
        "total_revenue": float(revenue_row["total_revenue"] or 0),
        "total_orders": int(revenue_row["total_orders"] or 0),
        "avg_order_value": float(revenue_row["avg_order_value"] or 0),
        "batch_time": batch_time
    }

    # ─── 3. Event type breakdown ───
    event_counts = batch_df.groupBy("event_type") \
        .count() \
        .collect()

    event_breakdown = {
        row["event_type"]: row["count"]
        for row in event_counts
    }

    # ─── 4. Top products ───
    top_products = batch_df \
        .filter(col("event_type").isin(["product_view", "add_to_cart", "purchase"])) \
        .groupBy("product_name", "product_category") \
        .agg(
            count("event_id").alias("interactions"),
            spark_sum(when(col("event_type") == "purchase",
                          col("revenue")).otherwise(0)).alias("revenue")
        ) \
        .orderBy(col("interactions").desc()) \
        .limit(5) \
        .collect()

    top_products_data = [
        {
            "product": row["product_name"],
            "category": row["product_category"],
            "interactions": row["interactions"],
            "revenue": float(row["revenue"])
        }
        for row in top_products
    ]

    # ─── 5. Device breakdown ───
    device_counts = batch_df.groupBy("device") \
        .count() \
        .collect()

    device_breakdown = {row["device"]: row["count"] for row in device_counts}

    # ─── 6. Conversion funnel ───
    funnel = {
        "page_views": event_breakdown.get("page_view", 0),
        "product_views": event_breakdown.get("product_view", 0),
        "add_to_cart": event_breakdown.get("add_to_cart", 0),
        "checkout_start": event_breakdown.get("checkout_start", 0),
        "purchases": event_breakdown.get("purchase", 0),
    }

    # Calculate conversion rates
    if funnel["page_views"] > 0:
        funnel["view_to_cart_rate"] = round(
            funnel["add_to_cart"] / funnel["page_views"] * 100, 2
        )
        funnel["cart_to_purchase_rate"] = round(
            funnel["purchases"] / max(funnel["add_to_cart"], 1) * 100, 2
        )

    # ─── 7. Country breakdown ───
    country_counts = batch_df.groupBy("country") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(5) \
        .collect()

    country_breakdown = {row["country"]: row["count"] for row in country_counts}

    # ─── 8. Write all to Redis ───
    redis_writer.write_metrics({
        "revenue": revenue_metrics,
        "event_breakdown": event_breakdown,
        "top_products": top_products_data,
        "device_breakdown": device_breakdown,
        "funnel": funnel,
        "country_breakdown": country_breakdown,
        "total_events": count_val,
        "last_updated": batch_time
    })

    # ─── 9. Print summary ───
    logger.info(f"💰 Revenue: ₹{revenue_metrics['total_revenue']:,.0f} | "
               f"Orders: {revenue_metrics['total_orders']}")
    logger.info(f"📊 Events: {event_breakdown}")
    logger.info(f"🔄 Funnel: {funnel}")
    logger.info(f"📱 Devices: {device_breakdown}")
    logger.info(f"🌍 Countries: {country_breakdown}")
    logger.info(f"🏆 Top product: {top_products_data[0]['product'] if top_products_data else 'N/A'}")


# ============================================
# MAIN STREAMING JOB
# ============================================
def run_streaming_job():
    """Main Spark Streaming entry point"""

    spark = create_spark_session()

    print(f"\n{'='*60}")
    print(f"⚡ PYSPARK STREAMING JOB STARTED")
    print(f"{'='*60}")
    print(f"Kafka:     {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}")
    print(f"Batch:     {BATCH_INTERVAL}")
    print(f"Outputs:   Redis + Elasticsearch + PostgreSQL")
    print(f"{'='*60}\n")

    # ─── Read from Kafka ───
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # ─── Parse JSON ───
    parsed_stream = raw_stream.select(
        from_json(
            col("value").cast("string"),
            EVENT_SCHEMA
        ).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # ─── Write stream with foreachBatch ───
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime=BATCH_INTERVAL) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/main") \
        .start()

    logger.info("✅ Streaming query started — waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_job()