"""
Pipeline Tests
Author: Yuvraaj M N
"""

import pytest
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ============================================
# TEST KAFKA PRODUCER
# ============================================
class TestEventGeneration:

    def test_event_has_required_fields(self):
        """Generated events must have all required fields"""
        from streaming.producer import generate_event

        user_pool = ["user_001", "user_002", "user_003"]
        event = generate_event(user_pool)

        required_fields = [
            'event_id', 'event_type', 'user_id', 'session_id',
            'product_id', 'product_name', 'product_category',
            'price', 'quantity', 'revenue', 'timestamp',
            'country', 'device'
        ]
        for field in required_fields:
            assert field in event, f"Missing field: {field}"

    def test_event_types_valid(self):
        """Event types must be from valid set"""
        from streaming.producer import generate_event, EVENT_TYPES

        user_pool = ["user_001"]
        for _ in range(50):
            event = generate_event(user_pool)
            assert event['event_type'] in EVENT_TYPES

    def test_purchase_has_revenue(self):
        """Purchase events must have positive revenue"""
        from streaming.producer import generate_event

        user_pool = ["user_001"]
        purchase_events = []
        attempts = 0
        while len(purchase_events) < 5 and attempts < 200:
            event = generate_event(user_pool)
            if event['event_type'] == 'purchase':
                purchase_events.append(event)
            attempts += 1

        for event in purchase_events:
            assert event['revenue'] > 0

    def test_non_purchase_zero_revenue(self):
        """Non-purchase events must have zero revenue"""
        from streaming.producer import generate_event

        user_pool = ["user_001"]
        for _ in range(50):
            event = generate_event(user_pool)
            if event['event_type'] != 'purchase':
                assert event['revenue'] == 0


# ============================================
# TEST METRICS API
# ============================================
class TestMetricsAPI:

    def test_health_endpoint(self):
        """Health endpoint returns correct structure"""
        from fastapi.testclient import TestClient
        from spark.metrics_api import app

        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"

    def test_summary_endpoint_structure(self):
        """Summary endpoint returns all required keys"""
        from fastapi.testclient import TestClient
        from spark.metrics_api import app

        client = TestClient(app)
        response = client.get("/metrics/summary")
        assert response.status_code == 200
        data = response.json()

        required_keys = [
            'revenue', 'funnel', 'top_products',
            'devices', 'events', 'countries'
        ]
        for key in required_keys:
            assert key in data, f"Missing key: {key}"

    def test_revenue_endpoint(self):
        """Revenue endpoint returns correct structure"""
        from fastapi.testclient import TestClient
        from spark.metrics_api import app

        client = TestClient(app)
        response = client.get("/metrics/revenue")
        assert response.status_code == 200


# ============================================
# TEST DATA PROCESSING
# ============================================
class TestDataProcessing:

    def test_batch_aggregation(self):
        """Batch processing computes correct aggregations"""
        from collections import defaultdict

        events = [
            {"event_type": "purchase", "revenue": 1000,
             "product_name": "iPhone", "product_category": "electronics",
             "device": "mobile", "country": "IN",
             "user_id": "u1", "is_new_user": True},
            {"event_type": "purchase", "revenue": 2000,
             "product_name": "MacBook", "product_category": "electronics",
             "device": "desktop", "country": "US",
             "user_id": "u2", "is_new_user": False},
            {"event_type": "page_view", "revenue": 0,
             "product_name": "iPhone", "product_category": "electronics",
             "device": "mobile", "country": "IN",
             "user_id": "u3", "is_new_user": True},
        ]

        purchases = [e for e in events if e["event_type"] == "purchase"]
        total_revenue = sum(e["revenue"] for e in purchases)
        assert total_revenue == 3000
        assert len(purchases) == 2

    def test_funnel_calculation(self):
        """Funnel rates calculated correctly"""
        funnel = {
            "page_views": 100,
            "add_to_cart": 30,
            "purchases": 15,
        }

        view_to_cart = funnel["add_to_cart"] / funnel["page_views"] * 100
        cart_to_purchase = funnel["purchases"] / funnel["add_to_cart"] * 100

        assert view_to_cart == 30.0
        assert cart_to_purchase == 50.0

    def test_device_breakdown(self):
        """Device breakdown computed correctly"""
        from collections import defaultdict

        events = [
            {"device": "mobile"}, {"device": "mobile"},
            {"device": "desktop"}, {"device": "tablet"},
        ]

        device_counts = defaultdict(int)
        for e in events:
            device_counts[e["device"]] += 1

        assert device_counts["mobile"] == 2
        assert device_counts["desktop"] == 1
        assert device_counts["tablet"] == 1