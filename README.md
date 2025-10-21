# 🚀 TeleStream Insights Hub

> **Production-grade data engineering platform for telecom analytics** - Real-time & batch processing with modern data stack

[![CI/CD](https://github.com/yourorg/telestream/workflows/CI/badge.svg)](https://github.com/yourorg/telestream/actions)
[![Code Coverage](https://codecov.io/gh/yourorg/telestream/branch/main/graph/badge.svg)](https://codecov.io/gh/yourorg/telestream)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [API Documentation](#api-documentation)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

TeleStream Insights Hub is an end-to-end data platform that processes **10M+ CDR records daily** with both batch and streaming capabilities. Built for a telecom enterprise undergoing digital transformation.

### Key Capabilities

- ✅ **Dual Processing**: Batch (Spark) + Streaming (Kafka/Flink)
- ✅ **Lakehouse Architecture**: Delta Lake with Bronze-Silver-Gold layers
- ✅ **Production-Grade**: CI/CD, testing, monitoring, data quality
- ✅ **Real-time Analytics**: Sub-30s latency for network events
- ✅ **Self-Service**: REST API + dashboards for business users
- ✅ **Scalable**: Processes 100K+ events/sec with auto-scaling

### Use Cases

1. **Customer Usage Analytics** - Track data/voice/SMS consumption
2. **Network Performance Monitoring** - Real-time cell tower health
3. **Anomaly Detection** - Identify unusual usage patterns
4. **Revenue Optimization** - Customer segmentation and targeting
5. **Network Capacity Planning** - Predictive analytics

---

## 🏗️ Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Billing   │───▶│   Ingestion  │───▶│  Data Lake  │
│   System    │    │   (Batch/    │    │   (Delta)   │
└─────────────┘    │   Stream)    │    └──────┬──────┘
                   └──────────────┘           │
┌─────────────┐                               │
│   Network   │───────────┐                   ▼
│  Monitoring │           │         ┌─────────────────┐
└─────────────┘           └────────▶│  Processing     │
                                    │  (Spark/Kafka)  │
┌─────────────┐                     └────────┬────────┘
│   Customer  │                              │
│     CRM     │                              ▼
└─────────────┘                   ┌──────────────────┐
                                  │   Warehouse +    │
                                  │     Cache        │
                                  └────────┬─────────┘
                                           │
                    ┌──────────────────────┼──────────────────┐
                    ▼                      ▼                  ▼
              ┌──────────┐          ┌──────────┐      ┌──────────┐
              │   API    │          │Dashboard │      │Notebooks │
              └──────────┘          └──────────┘      └──────────┘
```

**Technology Stack**:
- **Ingestion**: Kafka, Spark Batch
- **Processing**: PySpark, Spark Structured Streaming
- **Storage**: Delta Lake, PostgreSQL, Redis
- **Orchestration**: Apache Airflow
- **API**: FastAPI (async)
- **Analytics**: Metabase, Jupyter
- **Monitoring**: Prometheus, Grafana, ELK
- **Deployment**: Docker, Kubernetes, Terraform

---

## ✨ Features

### Data Engineering
- 🔄 **Medallion Architecture** - Bronze/Silver/Gold layers
- 🎯 **Schema Evolution** - Backward compatible changes
- 🔐 **Data Quality** - Great Expectations integration
- 📊 **Star Schema** - Optimized dimensional model
- 🕐 **Time Travel** - Delta Lake versioning
- 🔍 **Data Lineage** - OpenLineage tracking

### Real-Time Processing
- ⚡ **Sub-minute Latency** - Stream processing with Kafka
- 📈 **Windowed Aggregations** - Tumbling/sliding windows
- 🚨 **Anomaly Detection** - Statistical models
- 💾 **Stateful Processing** - RocksDB state store
- 🎪 **Exactly-Once Semantics** - Guaranteed delivery

### API & Analytics
- 🚀 **High-Performance API** - Async FastAPI with caching
- 🔑 **Authentication** - API keys + JWT tokens
- ⏱️ **Rate Limiting** - 100 req/min per key
- 📱 **OpenAPI Docs** - Interactive Swagger UI
- 📊 **BI Dashboards** - Metabase integration

### DevOps & Observability
- 🔄 **CI/CD Pipeline** - GitHub Actions
- 🧪 **Comprehensive Testing** - Unit, integration, load tests
- 📈 **Monitoring** - Prometheus + Grafana
- 📝 **Logging** - ELK Stack
- 🚨 **Alerting** - PagerDuty, Slack integration

---

## 📦 Prerequisites

### Required Software
```bash
# Core dependencies
- Docker 20.10+
- Docker Compose 2.0+
- Python 3.9+
- Make (optional)

# For local development
- Apache Spark 3.5+
- Java 11
- Poetry or pip
```

### Cloud Resources (Optional)
- AWS Account (S3, EMR) or
- GCP Account (GCS, Dataproc) or
- Azure Account (Blob Storage, HDInsight)

---

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/yourorg/telestream-insights-hub.git
cd telestream-insights-hub
```

### 2. Set Up Environment
```bash
# Copy environment template
cp .env.example .env

# Edit configuration
vim .env

# Install Python dependencies
pip install -r requirements.txt

# Or using Poetry
poetry install
```

### 3. Start Infrastructure
```bash
# Start all services with Docker Compose
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs
docker-compose logs -f
```

### 4. Initialize Database
```bash
# Run database migrations
docker-compose exec postgres psql -U telestream -d telestream_dw -f /init-scripts/schema.sql

# Create Airflow connections
docker-compose exec airflow-webserver airflow connections add \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema telestream_dw \
  --conn-login telestream \
  --conn-password telestream123 \
  telestream_warehouse
```

### 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | admin / admin123 |
| API Docs | http://localhost:8000/api/docs | API Key required |
| Grafana | http://localhost:3001 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |

---

## 📁 Project Structure

```
tele-stream-insights-hub/
├── ingestion/
│   ├── batch_ingest.py           # Batch data ingestion
│   └── kafka_producer.py         # Stream event producer
├── processing/
│   ├── batch_transform.py        # Spark batch ETL
│   └── streaming_processor.py    # Kafka streaming pipeline
├── storage/
│   ├── warehouse_loader.py       # Data warehouse loader
│   └── lake_writer.py            # Delta Lake writer
├── orchestration/
│   ├── airflow_dag.py            # Production DAG
│   └── prefect_flow.py           # Alternative orchestration
├── api/
│   ├── fastapi_server.py         # REST API service
│   ├── models.py                 # Pydantic models
│   └── auth.py                   # Authentication
├── tests/
│   ├── unit/                     # Unit tests
│   ├── integration/              # Integration tests
│   └── performance/              # Load tests
├── dashboard/
│   ├── metabase_config.json      # Dashboard configs
│   └── superset_dashboards/      # Apache Superset
├── notebooks/
│   └── exploratory_analysis.ipynb
├── docs/
│   ├── HLD.md                    # High-level design
│   ├── LLD.md                    # Low-level design
│   └── API.md                    # API documentation
├── monitoring/
│   ├── prometheus.yml            # Metrics config
│   ├── grafana_dashboards/       # Dashboard JSONs
│   └── alerts.yml                # Alert rules
├── infrastructure/
│   ├── terraform/                # Infrastructure as Code
│   │   ├── aws/
│   │   ├── gcp/
│   │   └── azure/
│   └── kubernetes/               # K8s manifests
│       ├── deployments/
│       ├── services/
│       └── helm/
├── .github/
│   └── workflows/
│       ├── ci.yml                # CI pipeline
│       ├── cd.yml                # CD pipeline
│       └── tests.yml             # Test automation
├── docker-compose.yml            # Local development
├── Dockerfile                    # Multi-stage build
├── requirements.txt              # Python dependencies
├── pyproject.toml                # Poetry config
├── setup.py                      # Package setup
├── Makefile                      # Common commands
└── README.md                     # This file
```

---

## ⚙️ Configuration

### Environment Variables

```bash
# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=telestream_dw
DB_USER=telestream
DB_PASSWORD=telestream123

# Kafka Configuration
KAFKA_BROKERS=kafka:9092
KAFKA_SCHEMA_REGISTRY=http://schema-registry:8081

# Storage Configuration
DATALAKE_PATH=s3://telestream-datalake
WAREHOUSE_PATH=s3://telestream-warehouse
AWS_REGION=us-east-1

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4
API_KEY_SALT=your-secret-salt

# Monitoring
PROMETHEUS_URL=http://prometheus:9090
GRAFANA_URL=http://grafana:3000

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
```

### Spark Configuration

Create `spark-defaults.conf`:
```properties
spark.sql.adaptive.enabled                     true
spark.sql.adaptive.coalescePartitions.enabled  true
spark.databricks.delta.optimizeWrite.enabled   true
spark.databricks.delta.autoCompact.enabled     true
spark.sql.parquet.compression.codec            snappy
spark.default.parallelism                      200
spark.sql.shuffle.partitions                   200
```

---

## 🎮 Running the Pipeline

### Manual Execution

#### 1. Batch Ingestion
```bash
# Ingest CDR data
python processing/batch_transform.py \
  --env prod \
  --action ingest \
  --source-path s3://raw-data/cdr/2025-10-20/ \
  --batch-id BATCH-20251020-001

# Check logs
tail -f logs/batch_transform.log
```

#### 2. Stream Processing
```bash
# Start streaming pipeline
python processing/streaming_processor.py \
  --mode consume \
  --kafka-brokers kafka:9092

# In another terminal, produce test events
python processing/streaming_processor.py \
  --mode produce \
  --num-cells 100 \
  --duration 60
```

#### 3. Transformations
```bash
# Bronze to Silver
python processing/batch_transform.py \
  --env prod \
  --action transform \
  --date 2025-10-20

# Silver to Gold
python processing/batch_transform.py \
  --env prod \
  --action aggregate \
  --date 2025-10-20
```

### Using Airflow

```bash
# Trigger DAG manually
docker-compose exec airflow-webserver \
  airflow dags trigger telestream_cdr_batch_pipeline \
  --conf '{"date": "2025-10-20"}'

# Monitor DAG run
docker-compose exec airflow-webserver \
  airflow dags list-runs -d telestream_cdr_batch_pipeline

# View task logs
docker-compose exec airflow-webserver \
  airflow tasks logs telestream_cdr_batch_pipeline \
  transform_bronze_to_silver \
  2025-10-20
```

### Using Make Commands

```bash
# Run complete pipeline
make run-pipeline DATE=2025-10-20

# Run tests
make test

# Start services
make up

# Stop services
make down

# View logs
make logs SERVICE=airflow-scheduler

# Clean all data
make clean
```

---

## 🧪 Testing

### Run All Tests
```bash
# Using pytest
pytest tests/ -v --cov=processing --cov-report=html

# Using Make
make test

# Run specific test suite
pytest tests/unit/ -v
pytest tests/integration/ -v -m integration
pytest tests/performance/ -v -m performance
```

### Test Categories

#### Unit Tests
```bash
# Test data transformations
pytest tests/unit/test_transformations.py -v

# Test data quality rules
pytest tests/unit/test_data_quality.py -v
```

#### Integration Tests
```bash
# Test end-to-end pipeline
pytest tests/integration/test_pipeline.py -v -m integration

# Test API endpoints
pytest tests/integration/test_api.py -v
```

#### Performance Tests
```bash
# Load test API
pytest tests/performance/test_load.py -v -m performance

# Benchmark Spark jobs
pytest tests/performance/test_spark_perf.py -v
```

### Code Quality

```bash
# Linting
flake8 processing/ api/ --max-line-length=120

# Type checking
mypy processing/ api/ --ignore-missing-imports

# Security scan
bandit -r processing/ api/

# Format code
black processing/ api/
isort processing/ api/
```

---

## 📊 Monitoring

### Metrics Dashboard

Access Grafana at `http://localhost:3001` to view:

1. **Pipeline Metrics**
   - Records processed per minute
   - Processing latency (P50, P95, P99)
   - Error rates
   - Data quality scores

2. **Infrastructure Metrics**
   - CPU/Memory utilization
   - Disk I/O
   - Network throughput
   - Kafka lag

3. **Business Metrics**
   - Total revenue
   - Active customers
   - Network health score
   - Anomaly count

### Alerts

Configured alerts trigger on:
- Pipeline failure (PagerDuty)
- Data quality < 95% (Slack)
- API latency > 1s (Email)
- Disk usage > 80% (Slack)

---

## 📚 API Documentation

### Authentication

```bash
# Get API key (admin only)
curl -X POST http://localhost:8000/api/v1/auth/keys \
  -H "Authorization: Bearer <admin-token>" \
  -d '{"name": "my-app", "role": "analyst"}'
```

### Example Requests

```bash
# Get real-time metrics
curl http://localhost:8000/api/v1/metrics/realtime \
  -H "X-API-Key: test-api-key-12345"

# Get customer usage
curl "http://localhost:8000/api/v1/customers/CUST-0001/usage?start_date=2025-01-01&end_date=2025-01-31" \
  -H "X-API-Key: test-api-key-12345"

# Get top customers
curl "http://localhost:8000/api/v1/customers/top?limit=10&order_by=revenue" \
  -H "X-API-Key: test-api-key-12345"

# Get network performance
curl "http://localhost:8000/api/v1/network/performance?technology=5G&min_health_score=50" \
  -H "X-API-Key: test-api-key-12345"
```

Full API documentation: http://localhost:8000/api/docs

---

## 🚢 Deployment

### Docker

```bash
# Build image
docker build -t telestream:latest .

# Push to registry
docker tag telestream:latest your-registry/telestream:v1.0.0
docker push your-registry/telestream:v1.0.0
```

### Kubernetes

```bash
# Deploy with Helm
helm install telestream ./infrastructure/kubernetes/helm/telestream \
  --namespace telestream \
  --create-namespace \
  --values values-prod.yaml

# Check status
kubectl get pods -n telestream
kubectl logs -f deployment/telestream-api -n telestream
```

### Terraform

```bash
# Initialize
cd infrastructure/terraform/aws
terraform init

# Plan
terraform plan -var-file=prod.tfvars

# Apply
terraform apply -var-file=prod.tfvars
```

---

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Standards

- Follow PEP 8 style guide
- Write unit tests for new features
- Update documentation
- Add type hints
- Keep functions < 50 lines

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👥 Team

- **Project Lead**: Data Engineering Team
- **Contributors**: See [CONTRIBUTORS.md](CONTRIBUTORS.md)
- **Support**: data-team@telestream.com

---

## 📞 Support

- **Documentation**: https://docs.telestream.io
- **Issues**: https://github.com/yourorg/telestream/issues
- **Slack**: #telestream-support
- **Email**: support@telestream.com

---

## 🎓 Learning Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [FastAPI Tutorial](https://fastapi.tiangolo.com/tutorial/)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

---

**⭐ If this project helps you, please star it on GitHub!**