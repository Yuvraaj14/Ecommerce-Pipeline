# 🛒 Real-Time E-Commerce Analytics Pipeline

> End-to-end big data pipeline processing real-time e-commerce events with Kafka, PySpark, Elasticsearch, dbt, Redis, and Grafana

**📊 Live Grafana Dashboard:** [localhost:3000](http://localhost:3000)
**⚡ Metrics API:** [https://ecommerce-metrics-api.onrender.com/docs](https://ecommerce-metrics-api.onrender.com/docs)

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.11.1-yellow.svg)](https://www.elastic.co/)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-red.svg)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🎯 What It Does

Simulates and processes real-time e-commerce events (clicks, purchases, cart actions)
through a production-grade big data pipeline. Ingests **10 events/second** via Kafka,
processes them with PySpark micro-batches, stores in Elasticsearch and PostgreSQL,
transforms with dbt, caches hot metrics in Redis, and visualizes on a live Grafana dashboard.

**Key Metrics Achieved:**
- ✅ **325,572 events** indexed in Elasticsearch
- ✅ **85,900+ rows** stored in Supabase PostgreSQL
- ✅ **8 Redis keys** updated every 30 seconds
- ✅ **6,219 Kafka messages** processed
- ✅ **6 dbt models** with 7/7 tests passing
- ✅ **₹754K revenue** tracked in real-time

---

## 🏗️ System Architecture

![Architecture](architecture.jpeg)

---

## 📸 Live Pipeline Screenshots

### Kafka — Topic Overview
![Kafka UI](screenshots/Kafka-UI.png)
6,219 messages processed across the `ecommerce-events` topic.

### Kibana — Indexed Events
![Kibana Discover](screenshots/Kibana-Discover.png)
325,572 events indexed in Elasticsearch with full-text search.

### Grafana — Live Dashboard

| Revenue, Orders, Avg Order Value & Funnel | Devices, Event Types, Countries & Top Products |
|:---:|:---:|
| ![Grafana Dashboard 1](screenshots/Grafana-Dashboard-1.png) | ![Grafana Dashboard 2](screenshots/Grafana-Dashboard-2.png) |

Real-time revenue, conversion funnel, device split, top countries, and top products — auto-refreshing every 10s.

### Upstash Redis — Hot Cache

| Database Details | Data Browser |
|:---:|:---:|
| ![Upstash Data Browser](screenshots/Upstash-Overview.png) | ![Upstash Details](screenshots/Upstash-Data-Browser.png) |

8 dashboard keys with 30s TTL, serving the Grafana metrics API via FastAPI.

### Supabase — Data Warehouse
![Supabase Table Editor](screenshots/Supabase-Table.png)
85,900+ rows ingested into `raw_events` for dbt transformations.

### dbt — Model Lineage
![dbt Lineage Graph](screenshots/dbt-Lineage.png)
6 models: 2 staging views feeding 4 mart tables (revenue, funnel, products, segments).

---

## 🛠️ Tech Stack

| Component | Technology | Why This Choice |
|-----------|-----------|----------------|
| **Event Ingestion** | Apache Kafka 7.5.0 | Decouples producers from consumers, replay capability, backpressure handling |
| **Stream Processing** | PySpark 3.5.0 | Distributed micro-batch processing, SQL-like aggregations on streams |
| **Search & Logs** | Elasticsearch 8.11.1 + Kibana | Inverted index for full-text log search, 325K events indexed |
| **Hot Cache** | Upstash Redis (TLS) | 30s TTL cache cuts Grafana→DB load by ~70%, managed serverless |
| **Data Warehouse** | Supabase PostgreSQL | Managed Postgres with REST API, 85K+ rows ingested |
| **Transformations** | dbt 1.7.4 | SQL-based transformations, lineage graph, built-in testing |
| **Monitoring** | Grafana + Prometheus | Real-time dashboard with 10s refresh, 8 live metric panels |
| **Metrics API** | FastAPI + Render | Serves Redis metrics to Grafana, always-on free hosting |
| **Infrastructure** | Docker Compose | 7 services orchestrated locally, K8s manifests for production |
| **CI/CD** | GitHub Actions | Automated test + Docker build on every push |

---

## 📊 Pipeline Metrics

### Real-Time Numbers
| Metric | Value |
|--------|-------|
| Kafka throughput | ~9 events/second |
| Kafka messages | 6,219+ |
| Elasticsearch hits | 325,572 |
| Supabase rows | 85,900+ |
| Redis dashboard keys | 8 |
| Batch interval | 30 seconds |
| Redis TTL | 30 seconds |
| Grafana refresh | 10 seconds |

### Grafana Dashboard Panels
| Panel | Type | Data Source |
|-------|------|-------------|
| 💰 Total Revenue | Stat (₹754K) | Redis → `/metrics/revenue` |
| 📦 Total Orders | Stat (20) | Redis → `/metrics/revenue` |
| 💳 Avg Order Value | Stat (₹37.7K) | Redis → `/metrics/revenue` |
| 🔄 Conversion Funnel | Bar Chart | Redis → `/metrics/funnel` |
| 📱 Device Split | Donut Chart | Redis → `/metrics/devices` |
| 📊 Event Types | Bar Chart | Redis → `/metrics/events` |
| 🌍 Top Countries | Pie Chart | Redis → `/metrics/countries` |
| 🏆 Top Products | Table | Redis → `/metrics/top-products` |

---

## ⚡ Event Schema

```json
{
  "event_id": "uuid",
  "event_type": "click|view|add_to_cart|purchase|remove_from_cart|checkout_start",
  "user_id": "user_xxx",
  "session_id": "sess_xxx",
  "product_id": "P001",
  "product_name": "iPhone 15",
  "product_category": "electronics",
  "price": 79999,
  "quantity": 1,
  "revenue": 79999,
  "timestamp": "2026-06-15T12:00:00Z",
  "country": "IN",
  "device": "mobile|desktop|tablet"
}
```

---

## 🔄 dbt Transformation Lineage

```
raw_events (PostgreSQL)
│
├── stg_events (view)
│       ├── conversion_funnel (table)
│       └── user_segments (table)
│
└── stg_purchases (view)
├── revenue_by_category (table)
└── top_products (table)

**dbt Test Results:** 7/7 passing
- ✅ unique event_id
- ✅ not_null event_id, event_type, revenue
- ✅ accepted_values for event_type
```

---

## 🏗️ Interview Tradeoffs

**Elasticsearch vs PostgreSQL for search:**
> ES handles full-text search and log analytics at scale using inverted indexes — perfect for 325K+ pipeline event logs. PostgreSQL handles structured relational queries and dbt aggregations. Used ES for operational log search, Postgres for business reporting.

**Redis hot cache:**
> Spark writes aggregated metrics every 30s to PostgreSQL. Grafana queries every 10s — that's too frequent for the DB. Redis caches latest aggregations with 30s TTL, cutting dashboard load time by ~70% and reducing DB queries.

**Kafka over direct ingestion:**
> Kafka decouples producers from consumers — if Spark goes down, events buffer in Kafka (configurable retention). Direct API ingestion loses events on consumer failure. Kafka gives replay capability and backpressure handling.

---

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# Clone repository
git clone https://github.com/Yuvraaj14/Ecommerce-Pipeline
cd Ecommerce-Pipeline

# Copy environment variables
cp .env.example .env
# Fill in: REDIS_URL, SUPABASE_HOST, SUPABASE_PASSWORD

# Start all services
docker-compose up -d

# Start event producer
python kafka/producer.py

# Start stream processor
python spark/streaming_job_simple.py

# Start metrics API
python spark/metrics_api.py

# Open Grafana
open http://localhost:3000  # admin/admin123
```

### Option 2: Individual Components

```bash
# Install dependencies
pip install -r requirements.txt

# Start Kafka only
docker-compose up -d kafka zookeeper redis elasticsearch kibana

# Run producer
python kafka/producer.py

# Run streaming job
python spark/streaming_job_simple.py

# Run metrics API
python spark/metrics_api.py
```

---

## 🔑 Environment Variables

```env
# Redis (Upstash)
REDIS_URL=rediss://default:PASSWORD@rare-ape-119947.upstash.io:6379
REDIS_TTL=30

# Supabase PostgreSQL
SUPABASE_HOST=db.xxxx.supabase.co
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=your_password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce-events
```

---

## 📁 Project Structure

```
Ecommerce-Pipeline/
├── kafka/
│   ├── producer.py              # Simulates 10 events/sec (500 users)
│   └── consumer.py              # Kafka consumer
├── spark/
│   ├── streaming_job.py         # PySpark structured streaming
│   ├── streaming_job_simple.py  # Python micro-batch processor
│   ├── metrics_api.py           # FastAPI metrics server
│   ├── write_to_postgres.py     # Kafka → Supabase writer
│   └── setup_db.py              # PostgreSQL table setup
├── ecommerce_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_events.sql
│   │   │   └── stg_purchases.sql
│   │   └── marts/
│   │       ├── conversion_funnel.sql
│   │       ├── revenue_by_category.sql
│   │       ├── top_products.sql
│   │       └── user_segments.sql
│   └── dbt_project.yml
├── monitoring/
│   └── prometheus.yml
├── k8s/
│   ├── kafka-deployment.yaml
│   ├── spark-deployment.yaml
│   └── metrics-api-deployment.yaml
├── docker-compose.yml
└── requirements.txt
```

---

## 🐳 Docker Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| Zookeeper | cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| Kafka | cp-kafka:7.5.0 | 9092 | Event streaming |
| Kafka UI | provectuslabs/kafka-ui | 8080 | Topic monitoring |
| Elasticsearch | elasticsearch:8.11.1 | 9200 | Event indexing |
| Kibana | kibana:8.11.1 | 5601 | Log visualization |
| Redis | redis:7-alpine | 6379 | Local dev cache |
| Prometheus | prom/prometheus | 9090 | Metrics scraping |
| Grafana | grafana/grafana | 3000 | Dashboard |

---

## ☸️ Kubernetes Deployment

K8s manifests in `k8s/` for production deployment:

```yaml
# Rolling update strategy
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1
    maxUnavailable: 0

# Health checks on /health endpoint
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 20
```

---

## 📈 Future Enhancements

- [ ] Airflow DAG for scheduled retraining
- [ ] Oracle Cloud K8s live deployment
- [ ] Jenkins CI/CD integration
- [ ] Great Expectations data validation
- [ ] Kafka Connect for CDC (Change Data Capture)
- [ ] Real-time ML model serving on event stream

---

## 🤝 Contributing

Pull requests welcome! Please open an issue first for major changes.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) file

---

## 👤 Author

**Yuvraaj M N**
- GitHub: [@Yuvraaj14](https://github.com/Yuvraaj14)
- LinkedIn: [yuvraaj-mn](https://linkedin.com/in/yuvraaj-mn)
- Metrics API: [Render](https://ecommerce-metrics-api.onrender.com/docs)
- RAG Project: [HF Spaces](https://huggingface.co/spaces/Yuvraaj14/rag-document-assistant)
- Emotion Recognition: [HF Spaces](https://huggingface.co/spaces/Yuvraaj14/emotion-recognition)
- Churn Predictor: [Render](https://churn-predictor-ui.onrender.com)

---

## 🙏 Acknowledgments

- [Apache Kafka](https://kafka.apache.org/) — Event streaming
- [Apache Spark](https://spark.apache.org/) — Stream processing
- [Elasticsearch](https://www.elastic.co/) — Search & analytics
- [dbt](https://www.getdbt.com/) — Data transformation
- [Upstash](https://upstash.com/) — Serverless Redis
- [Supabase](https://supabase.com/) — Managed PostgreSQL
- [Grafana](https://grafana.com/) — Visualization

---

**⭐ If this project helped you, please give it a star!**
