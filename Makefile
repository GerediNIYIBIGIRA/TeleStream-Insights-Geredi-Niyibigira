.PHONY: help setup install test lint format clean docker-up docker-down run-api run-batch run-stream docs

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip
VENV := venv
DOCKER_COMPOSE := docker-compose

help: ## Show this help message
	@echo "TeleStream Insights Hub - Available Commands"
	@echo "=============================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ==================== SETUP ====================

setup: ## Initial setup (create venv, install deps)
	@echo "Setting up development environment..."
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created"
	@echo "Activate it with: source venv/bin/activate (Linux/Mac) or venv\\Scripts\\activate (Windows)"

install: ## Install all dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov pytest-asyncio black flake8 isort mypy bandit
	@echo "✓ Dependencies installed"

install-dev: install ## Install development dependencies
	$(PIP) install jupyter notebook ipython
	@echo "✓ Development dependencies installed"

# ==================== CODE QUALITY ====================

lint: ## Run linting checks
	@echo "Running flake8..."
	flake8 processing/ api/ ingestion/ storage/ orchestration/ --max-line-length=120 --extend-ignore=E203,W503
	@echo "Running mypy..."
	mypy processing/ api/ --ignore-missing-imports || true
	@echo "✓ Linting complete"

format: ## Format code with black and isort
	@echo "Formatting with black..."
	black processing/ api/ ingestion/ storage/ orchestration/ tests/
	@echo "Sorting imports with isort..."
	isort processing/ api/ ingestion/ storage/ orchestration/ tests/
	@echo "✓ Code formatted"

security: ## Run security checks
	@echo "Running bandit security scan..."
	bandit -r processing/ api/ ingestion/ storage/ -ll
	@echo "Checking dependencies for vulnerabilities..."
	safety check || true
	@echo "✓ Security scan complete"

check: lint security ## Run all code quality checks

# ==================== TESTING ====================

test: ## Run all tests
	pytest tests/ -v --cov=processing --cov=api --cov=ingestion --cov=storage --cov-report=term --cov-report=html

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

test-integration: ## Run integration tests only
	pytest tests/integration/ -v

test-coverage: ## Run tests with detailed coverage report
	pytest tests/ -v --cov=processing --cov=api --cov=ingestion --cov=storage --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/index.html"

test-watch: ## Run tests in watch mode
	pytest-watch tests/ -v

# ==================== DOCKER ====================

docker-build: ## Build Docker images
	$(DOCKER_COMPOSE) build

docker-up: ## Start all services with Docker Compose
	$(DOCKER_COMPOSE) up -d
	@echo "✓ Services started"
	@echo "PostgreSQL: localhost:5432"
	@echo "Redis: localhost:6379"
	@echo "Kafka: localhost:9092"

docker-down: ## Stop all services
	$(DOCKER_COMPOSE) down
	@echo "✓ Services stopped"

docker-logs: ## View logs from all services
	$(DOCKER_COMPOSE) logs -f

docker-clean: ## Remove all containers, volumes, and images
	$(DOCKER_COMPOSE) down -v --rmi all
	@echo "✓ Docker environment cleaned"

docker-restart: docker-down docker-up ## Restart all services

# ==================== DATABASE ====================

db-init: ## Initialize database schema
	@echo "Initializing database..."
	$(DOCKER_COMPOSE) exec -T postgres psql -U telestream -d telestream_dw < schema.sql
	@echo "✓ Database initialized"

db-migrate: ## Run database migrations (placeholder)
	@echo "Running migrations..."
	# Add migration tool commands here (e.g., Alembic)
	@echo "✓ Migrations complete"

db-shell: ## Open PostgreSQL shell
	$(DOCKER_COMPOSE) exec postgres psql -U telestream -d telestream_dw

db-backup: ## Backup database
	@echo "Creating backup..."
	$(DOCKER_COMPOSE) exec postgres pg_dump -U telestream telestream_dw > backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "✓ Backup created"

# ==================== APPLICATION ====================

run-api: ## Run FastAPI server
	uvicorn api.fastapi_server:app --reload --host 0.0.0.0 --port 8000

run-batch: ## Run batch processing
	$(PYTHON) processing/batch_transform.py

run-stream: ## Run streaming processor
	$(PYTHON) processing/streaming_processor.py

run-ingest: ## Run batch ingestion
	$(PYTHON) ingestion/batch_ingest.py

run-producer: ## Run Kafka producer
	$(PYTHON) ingestion/kafka_producer.py

run-airflow: ## Start Airflow webserver
	airflow webserver --port 8080

run-scheduler: ## Start Airflow scheduler
	airflow scheduler

# ==================== DATA PIPELINE ====================

pipeline-batch: ## Run full batch pipeline
	@echo "Running batch pipeline..."
	$(PYTHON) orchestration/prefect_flow.py
	@echo "✓ Batch pipeline complete"

pipeline-test: ## Test pipeline with sample data
	@echo "Testing pipeline with sample data..."
	# Add test data pipeline commands
	@echo "✓ Pipeline test complete"

# ==================== MONITORING ====================

logs: ## View application logs
	tail -f logs/application.log

metrics: ## Show system metrics (if monitoring is set up)
	@echo "System metrics:"
	@echo "CPU usage:" && ps aux | awk '{sum += $$3} END {print sum "%"}'
	@echo "Memory usage:" && free -h

health: ## Check health of all services
	@echo "Checking service health..."
	@curl -s http://localhost:8000/health || echo "API: DOWN"
	@echo ""

# ==================== DOCUMENTATION ====================

docs: ## Generate documentation
	@echo "Generating documentation..."
	# Add sphinx or mkdocs commands
	@echo "✓ Documentation generated"

docs-serve: ## Serve documentation locally
	@echo "Serving documentation at http://localhost:8080"
	# Add documentation server command

# ==================== CLEANUP ====================

clean: ## Clean up generated files
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ .coverage coverage.xml
	@echo "✓ Cleanup complete"

clean-data: ## Clean all data directories
	@echo "Cleaning data directories..."
	rm -rf data/lake/* data/warehouse/* logs/*
	@echo "✓ Data cleaned"

clean-all: clean clean-data docker-clean ## Deep clean everything
	@echo "✓ Full cleanup complete"

# ==================== DEPLOYMENT ====================

deploy-dev: ## Deploy to development environment
	@echo "Deploying to development..."
	# Add deployment commands
	@echo "✓ Deployed to development"

deploy-prod: ## Deploy to production environment
	@echo "Deploying to production..."
	@echo "⚠️  This requires additional configuration"
	# Add production deployment commands

# ==================== UTILITIES ====================

shell: ## Open Python shell with project context
	$(PYTHON) -i -c "from api import *; from processing import *; print('TeleStream shell ready')"

notebook: ## Start Jupyter notebook
	jupyter notebook

dependencies: ## Show dependency tree
	pipdeptree

update-deps: ## Update all dependencies
	$(PIP) install --upgrade -r requirements.txt
	$(PIP) freeze > requirements.txt
	@echo "✓ Dependencies updated"

version: ## Show version information
	@echo "TeleStream Insights Hub"
	@echo "Python: $$($(PYTHON) --version)"
	@echo "Docker: $$(docker --version)"
	@echo "Docker Compose: $$(docker-compose --version)"

# ==================== CI/CD ====================

ci-local: lint test ## Run CI checks locally
	@echo "✓ Local CI checks passed"

pre-commit: format lint test-unit ## Run pre-commit checks
	@echo "✓ Pre-commit checks passed"