"""
Metrics API — reads from Redis → serves to Grafana
Author: Yuvraaj M N
"""

import json
import redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Gauge, generate_latest
from fastapi.responses import PlainTextResponse
import uvicorn
import os

app = FastAPI(title="Pipeline Metrics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

# Prometheus metrics
EVENTS_TOTAL = Counter('pipeline_events_total', 'Total events processed')
REVENUE_GAUGE = Gauge('pipeline_revenue_total', 'Total revenue in current batch')
ORDERS_GAUGE = Gauge('pipeline_orders_total', 'Total orders in current batch')


def get_from_redis(key: str):
    try:
        val = redis_client.get(f"dashboard:{key}")
        return json.loads(val) if val else {}
    except Exception:
        return {}


@app.get("/metrics/revenue")
def get_revenue():
    return get_from_redis("revenue")


@app.get("/metrics/funnel")
def get_funnel():
    return get_from_redis("funnel")


@app.get("/metrics/top-products")
def get_top_products():
    return get_from_redis("top_products")


@app.get("/metrics/devices")
def get_devices():
    return get_from_redis("device_breakdown")


@app.get("/metrics/events")
def get_events():
    return get_from_redis("event_breakdown")


@app.get("/metrics/countries")
def get_countries():
    return get_from_redis("country_breakdown")


@app.get("/metrics/summary")
def get_summary():
    return {
        "revenue": get_from_redis("revenue"),
        "funnel": get_from_redis("funnel"),
        "top_products": get_from_redis("top_products"),
        "devices": get_from_redis("device_breakdown"),
        "events": get_from_redis("event_breakdown"),
        "countries": get_from_redis("country_breakdown"),
        "last_updated": get_from_redis("last_updated")
    }

@app.get("/")
def root():
    return {
        "service": "ecommerce-metrics-api",
        "status": "running"
    }

@app.get("/prometheus", response_class=PlainTextResponse)
def prometheus_metrics():
    return generate_latest()


@app.get("/health")
def health():
    redis_ok = False
    try:
        redis_client.ping()
        redis_ok = True
    except Exception:
        pass
    return {
        "status": "healthy",
        "redis": redis_ok
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)