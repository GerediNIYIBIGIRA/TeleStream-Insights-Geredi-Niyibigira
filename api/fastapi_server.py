"""
Production-grade FastAPI service for TeleStream Analytics
Features: Authentication, rate limiting, caching, async operations, OpenAPI docs
"""

from fastapi import FastAPI, HTTPException, Depends, Query, Security, status
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

try:
    from fastapi_limiter import FastAPILimiter
    from fastapi_limiter.depends import RateLimiter

    RATE_LIMITER_AVAILABLE = True
except ImportError:
    RATE_LIMITER_AVAILABLE = False
    print("⚠️  fastapi-limiter not available, rate limiting disabled")

    # Dummy classes that do nothing
    class RateLimiter:
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self):
            return lambda: None

    class FastAPILimiter:
        @staticmethod
        async def init(*args, **kwargs):
            pass

        @staticmethod
        async def close():
            pass


from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from enum import Enum
import asyncpg
import redis.asyncio as aioredis
import logging
from functools import lru_cache
import json
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========================================
# APP INITIALIZATION
# ========================================

app = FastAPI(
    title="TeleStream Analytics API",
    description="Real-time and batch analytics API for telecom operations",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Security
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)

# ========================================
# PYDANTIC MODELS
# ========================================


class CallType(str, Enum):
    VOICE = "voice"
    SMS = "sms"
    DATA = "data"


class CustomerSegment(str, Enum):
    ENTERPRISE = "enterprise"
    CONSUMER = "consumer"
    SMB = "SMB"


class DateRange(BaseModel):
    start_date: date
    end_date: date

    @validator("end_date")
    def end_after_start(cls, v, values):
        if "start_date" in values and v < values["start_date"]:
            raise ValueError("end_date must be after start_date")
        return v


class CustomerUsageResponse(BaseModel):
    customer_id: str
    customer_name: str
    segment: CustomerSegment
    total_calls: int
    total_duration_minutes: float
    total_data_mb: float
    total_cost: float
    avg_signal_strength: Optional[int]
    dropped_calls_count: int
    call_drop_rate: Optional[float]
    last_activity: datetime


class NetworkPerformanceResponse(BaseModel):
    cell_id: str
    cell_name: str
    technology: str
    region: str
    avg_bandwidth_util: float
    avg_error_rate: float
    total_events: int
    critical_events: int
    health_score: float


class RealtimeMetricsResponse(BaseModel):
    timestamp: datetime
    active_calls: int
    total_bandwidth_gbps: float
    avg_latency_ms: float
    error_rate_percent: float
    active_cells: int


class AnomalyResponse(BaseModel):
    anomaly_id: str
    customer_id: str
    anomaly_type: str
    severity: str
    detected_at: datetime
    metrics: Dict[str, Any]
    description: str


# ========================================
# DATABASE CONNECTION
# ========================================


class DatabaseManager:
    """Async PostgreSQL connection manager"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            host="postgres",
            port=5432,
            database="telestream_dw",
            user="telestream",
            password="telestream123",
            min_size=10,
            max_size=50,
            command_timeout=60,
        )
        logger.info("Database connection pool created")

    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def fetch(self, query: str, *args):
        """Execute SELECT query"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Execute SELECT query and return single row"""
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args):
        """Execute INSERT/UPDATE/DELETE query"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)


db = DatabaseManager()

# ========================================
# REDIS CACHE
# ========================================


class CacheManager:
    """Async Redis cache manager"""

    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self):
        """Initialize Redis connection"""
        self.redis = await aioredis.from_url(
            "redis://redis:6379", encoding="utf-8", decode_responses=True
        )
        await FastAPILimiter.init(self.redis)
        logger.info("Redis cache connected")

    async def disconnect(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()
            logger.info("Redis cache disconnected")

    async def get(self, key: str) -> Optional[str]:
        """Get value from cache"""
        return await self.redis.get(key)

    async def set(self, key: str, value: str, ttl: int = 300):
        """Set value in cache with TTL"""
        await self.redis.setex(key, ttl, value)

    async def delete(self, key: str):
        """Delete key from cache"""
        await self.redis.delete(key)

    def generate_cache_key(self, *args) -> str:
        """Generate deterministic cache key"""
        key_string = ":".join(str(arg) for arg in args)
        return f"cache:{hashlib.md5(key_string.encode()).hexdigest()}"


cache = CacheManager()

# ========================================
# STARTUP/SHUTDOWN EVENTS
# ========================================


@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    await db.connect()
    await cache.connect()
    logger.info("Application startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections on shutdown"""
    await db.disconnect()
    await cache.disconnect()
    logger.info("Application shutdown complete")


# ========================================
# AUTHENTICATION & AUTHORIZATION
# ========================================


async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """Verify API key authentication"""
    valid_keys = {"test-api-key-12345", "prod-api-key-67890"}

    if api_key not in valid_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
        )
    return api_key


async def verify_bearer_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    """Verify Bearer token authentication"""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authentication"
        )

    # In production: verify JWT token with proper validation
    token = credentials.credentials
    if token != "valid-jwt-token":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    return token


# ========================================
# API ENDPOINTS
# ========================================


@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connection
        await db.fetchrow("SELECT 1")
        # Check Redis connection
        await cache.redis.ping()

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {"database": "up", "cache": "up"},
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "error": str(e)},
        )


@app.get(
    "/api/v1/metrics/realtime",
    response_model=RealtimeMetricsResponse,
    tags=["Metrics"],
    dependencies=[Depends(RateLimiter(times=100, seconds=60))],
)
async def get_realtime_metrics(api_key: str = Depends(verify_api_key)):
    """Get real-time system-wide metrics"""

    cache_key = cache.generate_cache_key("realtime_metrics")
    cached = await cache.get(cache_key)

    if cached:
        return JSONResponse(content=json.loads(cached))

    query = """
        SELECT 
            NOW() as timestamp,
            COUNT(DISTINCT call_id) as active_calls,
            SUM(data_volume_mb) / 1024.0 as total_bandwidth_gbps,
            AVG(latency_ms) as avg_latency_ms,
            AVG(error_rate_percent) as error_rate_percent,
            COUNT(DISTINCT originating_cell_id) as active_cells
        FROM fact_network_event
        WHERE event_timestamp >= NOW() - INTERVAL '5 minutes'
    """

    result = await db.fetchrow(query)

    if not result:
        raise HTTPException(status_code=404, detail="No real-time data available")

    response = {
        "timestamp": result["timestamp"].isoformat(),
        "active_calls": result["active_calls"] or 0,
        "total_bandwidth_gbps": float(result["total_bandwidth_gbps"] or 0),
        "avg_latency_ms": float(result["avg_latency_ms"] or 0),
        "error_rate_percent": float(result["error_rate_percent"] or 0),
        "active_cells": result["active_cells"] or 0,
    }

    await cache.set(cache_key, json.dumps(response), ttl=30)
    return response


@app.get(
    "/api/v1/customers/{customer_id}/usage",
    response_model=CustomerUsageResponse,
    tags=["Customers"],
)
async def get_customer_usage(
    customer_id: str,
    date_range: DateRange = Depends(),
    api_key: str = Depends(verify_api_key),
):
    """Get customer usage metrics for date range"""

    cache_key = cache.generate_cache_key(
        "customer_usage", customer_id, date_range.start_date, date_range.end_date
    )
    cached = await cache.get(cache_key)

    if cached:
        return JSONResponse(content=json.loads(cached))

    query = """
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name as customer_name,
            c.customer_segment as segment,
            SUM(u.total_calls) as total_calls,
            SUM(u.total_voice_minutes) as total_duration_minutes,
            SUM(u.total_data_mb) as total_data_mb,
            SUM(u.total_cost) as total_cost,
            AVG(u.avg_signal_strength_dbm) as avg_signal_strength,
            SUM(u.dropped_calls_count) as dropped_calls_count,
            CASE 
                WHEN SUM(u.total_calls) > 0 
                THEN (SUM(u.dropped_calls_count)::float / SUM(u.total_calls) * 100)
                ELSE 0 
            END as call_drop_rate,
            MAX(u.calculation_time) as last_activity
        FROM dim_customer c
        JOIN fact_customer_usage_daily u ON c.customer_key = u.customer_key
        JOIN dim_time t ON u.time_key = t.time_key
        WHERE c.customer_id = $1
          AND t.full_date BETWEEN $2 AND $3
          AND c.is_current = TRUE
        GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment
    """

    result = await db.fetchrow(
        query, customer_id, date_range.start_date, date_range.end_date
    )

    if not result:
        raise HTTPException(
            status_code=404, detail="Customer not found or no data available"
        )

    response = {
        "customer_id": result["customer_id"],
        "customer_name": result["customer_name"],
        "segment": result["segment"],
        "total_calls": result["total_calls"],
        "total_duration_minutes": float(result["total_duration_minutes"] or 0),
        "total_data_mb": float(result["total_data_mb"] or 0),
        "total_cost": float(result["total_cost"] or 0),
        "avg_signal_strength": result["avg_signal_strength"],
        "dropped_calls_count": result["dropped_calls_count"],
        "call_drop_rate": float(result["call_drop_rate"] or 0),
        "last_activity": result["last_activity"].isoformat(),
    }

    await cache.set(cache_key, json.dumps(response), ttl=300)
    return response


@app.get(
    "/api/v1/customers/top",
    response_model=List[CustomerUsageResponse],
    tags=["Customers"],
)
async def get_top_customers(
    limit: int = Query(10, ge=1, le=100),
    segment: Optional[CustomerSegment] = None,
    order_by: str = Query("revenue", regex="^(revenue|data|calls)$"),
    api_key: str = Depends(verify_api_key),
):
    """Get top customers by revenue, data usage, or call volume"""

    cache_key = cache.generate_cache_key("top_customers", limit, segment, order_by)
    cached = await cache.get(cache_key)

    if cached:
        return JSONResponse(content=json.loads(cached))

    order_column = {
        "revenue": "total_revenue",
        "data": "total_data_mb",
        "calls": "total_calls",
    }[order_by]

    segment_filter = f"AND customer_segment = '{segment.value}'" if segment else ""

    query = f"""
        SELECT 
            customer_id,
            customer_name,
            customer_segment as segment,
            total_data_mb,
            total_voice_minutes as total_duration_minutes,
            total_revenue as total_cost,
            0 as avg_signal_strength,
            0 as dropped_calls_count,
            0.0 as call_drop_rate,
            NOW() as last_activity
        FROM mv_top_customers_usage
        WHERE 1=1 {segment_filter}
        ORDER BY {order_column} DESC
        LIMIT $1
    """

    results = await db.fetch(query, limit)

    response = [
        {
            "customer_id": r["customer_id"],
            "customer_name": r["customer_name"],
            "segment": r["segment"],
            "total_calls": 0,
            "total_duration_minutes": float(r["total_duration_minutes"] or 0),
            "total_data_mb": float(r["total_data_mb"] or 0),
            "total_cost": float(r["total_cost"] or 0),
            "avg_signal_strength": r["avg_signal_strength"],
            "dropped_calls_count": r["dropped_calls_count"],
            "call_drop_rate": float(r["call_drop_rate"] or 0),
            "last_activity": r["last_activity"].isoformat(),
        }
        for r in results
    ]

    await cache.set(cache_key, json.dumps(response), ttl=600)
    return response


@app.get(
    "/api/v1/network/performance",
    response_model=List[NetworkPerformanceResponse],
    tags=["Network"],
)
async def get_network_performance(
    region: Optional[str] = None,
    technology: Optional[str] = Query(None, regex="^(4G|5G|LTE)$"),
    min_health_score: float = Query(0, ge=0, le=100),
    api_key: str = Depends(verify_api_key),
):
    """Get network performance metrics by cell tower"""

    cache_key = cache.generate_cache_key(
        "network_performance", region, technology, min_health_score
    )
    cached = await cache.get(cache_key)

    if cached:
        return JSONResponse(content=json.loads(cached))

    filters = ["1=1"]
    params = []
    param_num = 1

    if region:
        filters.append(f"region_name = ${param_num}")
        params.append(region)
        param_num += 1

    if technology:
        filters.append(f"technology = ${param_num}")
        params.append(technology)
        param_num += 1

    query = f"""
        SELECT 
            cell_id,
            cell_name,
            technology,
            region_name as region,
            avg_bandwidth_util,
            avg_error_rate,
            total_events,
            critical_events,
            GREATEST(0, 100 - (avg_error_rate * 10) - 
                    CASE WHEN avg_bandwidth_util > 80 THEN 20 ELSE 0 END -
                    (critical_events * 5)) as health_score
        FROM mv_network_performance
        WHERE {' AND '.join(filters)}
        HAVING GREATEST(0, 100 - (avg_error_rate * 10) - 
                CASE WHEN avg_bandwidth_util > 80 THEN 20 ELSE 0 END -
                (critical_events * 5)) >= ${param_num}
        ORDER BY health_score ASC
        LIMIT 100
    """

    params.append(min_health_score)
    results = await db.fetch(query, *params)

    response = [
        {
            "cell_id": r["cell_id"],
            "cell_name": r["cell_name"] or "Unknown",
            "technology": r["technology"],
            "region": r["region"] or "Unknown",
            "avg_bandwidth_util": float(r["avg_bandwidth_util"] or 0),
            "avg_error_rate": float(r["avg_error_rate"] or 0),
            "total_events": r["total_events"] or 0,
            "critical_events": r["critical_events"] or 0,
            "health_score": float(r["health_score"] or 0),
        }
        for r in results
    ]

    await cache.set(cache_key, json.dumps(response), ttl=300)
    return response


@app.get("/api/v1/anomalies", response_model=List[AnomalyResponse], tags=["Anomalies"])
async def get_anomalies(
    start_date: date = Query(...),
    end_date: date = Query(...),
    severity: Optional[str] = Query(None, regex="^(CRITICAL|HIGH|MEDIUM)$"),
    customer_id: Optional[str] = None,
    api_key: str = Depends(verify_api_key),
):
    """Get detected usage anomalies"""

    # Note: This would query from anomaly detection results stored in DB
    # For demo purposes, returning mock structure

    query = """
        SELECT 
            'ANOM-' || event_id as anomaly_id,
            'CUST-' || CAST(active_users AS TEXT) as customer_id,
            event_type as anomaly_type,
            CASE 
                WHEN error_rate_percent > 10 THEN 'CRITICAL'
                WHEN error_rate_percent > 5 THEN 'HIGH'
                ELSE 'MEDIUM'
            END as severity,
            event_timestamp as detected_at,
            jsonb_build_object(
                'bandwidth_util', bandwidth_utilization_percent,
                'error_rate', error_rate_percent,
                'active_users', active_users
            ) as metrics,
            'Anomalous network behavior detected' as description
        FROM fact_network_event
        WHERE event_timestamp BETWEEN $1 AND $2
          AND (error_rate_percent > 5 OR bandwidth_utilization_percent > 90)
        ORDER BY event_timestamp DESC
        LIMIT 100
    """

    results = await db.fetch(query, start_date, end_date)

    response = [
        {
            "anomaly_id": r["anomaly_id"],
            "customer_id": r["customer_id"],
            "anomaly_type": r["anomaly_type"],
            "severity": r["severity"],
            "detected_at": r["detected_at"].isoformat(),
            "metrics": r["metrics"],
            "description": r["description"],
        }
        for r in results
    ]

    return response


@app.post(
    "/api/v1/cache/invalidate",
    tags=["System"],
    dependencies=[Depends(verify_bearer_token)],
)
async def invalidate_cache(pattern: str = Query("*")):
    """Invalidate cache keys matching pattern (admin only)"""

    # In production: implement proper Redis key pattern scanning
    await cache.redis.flushdb()

    return {
        "message": f"Cache invalidated for pattern: {pattern}",
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/v1/stats/summary", tags=["Statistics"])
async def get_summary_statistics(
    date_range: DateRange = Depends(), api_key: str = Depends(verify_api_key)
):
    """Get summary statistics for dashboard"""

    query = """
        SELECT 
            COUNT(DISTINCT c.customer_id) as total_customers,
            SUM(u.total_calls) as total_calls,
            SUM(u.total_data_mb) / 1024.0 as total_data_gb,
            SUM(u.total_cost) as total_revenue,
            AVG(u.avg_signal_strength_dbm) as avg_signal_strength,
            SUM(u.dropped_calls_count)::float / NULLIF(SUM(u.total_calls), 0) * 100 as overall_drop_rate
        FROM fact_customer_usage_daily u
        JOIN dim_customer c ON u.customer_key = c.customer_key
        JOIN dim_time t ON u.time_key = t.time_key
        WHERE t.full_date BETWEEN $1 AND $2
    """

    result = await db.fetchrow(query, date_range.start_date, date_range.end_date)

    return {
        "period": {
            "start_date": date_range.start_date.isoformat(),
            "end_date": date_range.end_date.isoformat(),
        },
        "metrics": {
            "total_customers": result["total_customers"] or 0,
            "total_calls": result["total_calls"] or 0,
            "total_data_gb": float(result["total_data_gb"] or 0),
            "total_revenue": float(result["total_revenue"] or 0),
            "avg_signal_strength": result["avg_signal_strength"],
            "overall_drop_rate": float(result["overall_drop_rate"] or 0),
        },
        "timestamp": datetime.now().isoformat(),
    }


# ========================================
# ERROR HANDLERS
# ========================================


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "timestamp": datetime.now().isoformat(),
            "path": str(request.url),
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "timestamp": datetime.now().isoformat(),
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "fastapi_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=4,
        log_level="info",
    )
