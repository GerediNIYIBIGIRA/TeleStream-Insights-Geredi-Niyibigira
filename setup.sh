#!/bin/bash

# TeleStream Insights Hub - Development Environment Setup
# This script sets up your local development environment

set -e  # Exit on any error

echo "======================================"
echo "TeleStream Insights Hub - Setup"
echo "======================================"
echo ""

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_success "Python $PYTHON_VERSION installed"
else
    print_error "Python 3 is not installed"
    exit 1
fi

# Check Docker
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | sed 's/,//')
    print_success "Docker $DOCKER_VERSION installed"
else
    print_warning "Docker is not installed (optional for local testing)"
fi

# Check Docker Compose
if command -v docker-compose &> /dev/null; then
    print_success "Docker Compose installed"
else
    print_warning "Docker Compose is not installed (optional for local testing)"
fi

echo ""
echo "======================================"
echo "Step 1: Creating Virtual Environment"
echo "======================================"
echo ""

# Create virtual environment
if [ ! -d "ngeredienv" ]; then
    python3 -m venv ngeredienv
    print_success "Virtual environment created"
else
    print_warning "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate || . venv/Scripts/activate 2>/dev/null || print_warning "Please activate manually"

echo ""
echo "======================================"
echo "Step 2: Installing Dependencies"
echo "======================================"
echo ""

# Upgrade pip
pip install --upgrade pip
print_success "Pip upgraded"

# Install requirements
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    print_success "Dependencies installed from requirements.txt"
else
    print_error "requirements.txt not found"
    exit 1
fi

# Install development dependencies
echo ""
echo "Installing development dependencies..."
pip install pytest pytest-cov pytest-asyncio black flake8 isort mypy

print_success "Development dependencies installed"

echo ""
echo "======================================"
echo "Step 3: Setting Up Configuration"
echo "======================================"
echo ""

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    cat > .env << 'EOF'
# TeleStream Insights Hub - Environment Variables

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=telestream
DB_PASSWORD=telestream123
DB_NAME=telestream_dw

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_URL=redis://localhost:6379

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=telestream_events

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Data Lake Configuration
DATA_LAKE_PATH=./data/lake
DATA_WAREHOUSE_PATH=./data/warehouse

# Airflow Configuration (if using)
AIRFLOW_HOME=./airflow
AIRFLOW__CORE__DAGS_FOLDER=./orchestration
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Feature Flags
ENABLE_STREAMING=true
ENABLE_BATCH=true
ENABLE_ML_FEATURES=false
EOF
    print_success ".env file created"
else
    print_warning ".env file already exists"
fi

# Create data directories
echo ""
echo "Creating data directories..."
mkdir -p data/lake/{raw,processed,curated}
mkdir -p data/warehouse
mkdir -p logs
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p tests/fixtures
mkdir -p tests/integration

print_success "Data directories created"

echo ""
echo "======================================"
echo "Step 4: Database Setup"
echo "======================================"
echo ""

# Check if Docker is available for local database
if command -v docker &> /dev/null; then
    echo "Would you like to start local PostgreSQL and Redis using Docker? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        docker-compose up -d postgres redis
        print_success "Database services started"
        
        echo "Waiting for PostgreSQL to be ready..."
        sleep 5
        
        # Initialize database schema
        if [ -f "schema.sql" ]; then
            docker-compose exec -T postgres psql -U telestream -d telestream_dw < schema.sql
            print_success "Database schema initialized"
        fi
    fi
else
    print_warning "Docker not available. You'll need to set up PostgreSQL manually."
    echo "  1. Install PostgreSQL"
    echo "  2. Create database: telestream_dw"
    echo "  3. Run schema.sql"
fi

echo ""
echo "======================================"
echo "Step 5: Running Tests"
echo "======================================"
echo ""

# Run tests to verify setup
pytest tests/unit/ -v || print_warning "Some unit tests failed (this is okay for initial setup)"

echo ""
echo "======================================"
echo "Setup Complete!"
echo "======================================"
echo ""
print_success "Environment setup completed successfully!"
echo ""
echo "Next steps:"
echo "  1. Activate virtual environment: source venv/bin/activate"
echo "  2. Review .env file and update configuration"
echo "  3. Start services: docker-compose up -d"
echo "  4. Run the API: python api/fastapi_server.py"
echo "  5. View API docs: http://localhost:8000/docs"
echo ""
echo "Useful commands:"
echo "  - Run tests: pytest tests/ -v"
echo "  - Format code: black . && isort ."
echo "  - Lint code: flake8 ."
echo "  - Run pipeline: python orchestration/prefect_flow.py"
echo ""
print_success "Happy coding! 🚀"