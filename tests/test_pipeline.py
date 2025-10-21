"""
Comprehensive test suite for TeleStream pipeline
Includes: Unit tests, Integration tests, Load tests, Data quality tests
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import asyncio
from httpx import AsyncClient
import json
from kafka import KafkaProducer, KafkaConsumer
import time

# ========================================
# FIXTURES
# ========================================


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = (
        SparkSession.builder.appName("TeleStream-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def sample_cdr_data():
    """Generate sample CDR data for testing"""
    return pd.DataFrame(
        {
            "call_id": [f"CALL-{i:06d}" for i in range(1000)],
            "customer_id": [f"CUST-{i%100:04d}" for i in range(1000)],
            "call_type": ["voice", "sms", "data"] * 334,
            "call_direction": ["inbound", "outbound"] * 500,
            "call_start_time": [
                (datetime.now() - timedelta(hours=i)) for i in range(1000)
            ],
            "call_end_time": [
                (datetime.now() - timedelta(hours=i - 1)) for i in range(1000)
            ],
            "duration_seconds": [i * 60 for i in range(1000)],
            "data_volume_mb": [float(i * 0.5) for i in range(1000)],
            "signal_strength_dbm": [-70 - (i % 30) for i in range(1000)],
            "call_cost": [float(i * 0.1) for i in range(1000)],
        }
    )


@pytest.fixture
async def api_client():
    """Create async HTTP client for API testing"""
    async with AsyncClient(base_url="http://localhost:8000") as client:
        yield client


# ========================================
# UNIT TESTS - DATA TRANSFORMATIONS
# ========================================


class TestDataTransformations:
    """Test data transformation logic"""

    def test_schema_validation(self, spark_session, sample_cdr_data):
        """Test CDR schema validation"""
        df = spark_session.createDataFrame(sample_cdr_data)

        # Verify required columns exist
        required_columns = {"call_id", "customer_id", "call_type", "call_start_time"}
        assert required_columns.issubset(set(df.columns))

        # Verify no null values in key columns
        assert df.filter(df.call_id.isNull()).count() == 0
        assert df.filter(df.customer_id.isNull()).count() == 0

    def test_deduplication(self, spark_session, sample_cdr_data):
        """Test deduplication logic"""
        # Add duplicate records
        duplicated_data = pd.concat([sample_cdr_data, sample_cdr_data.head(10)])
        df = spark_session.createDataFrame(duplicated_data)

        # Apply deduplication
        df_dedup = df.dropDuplicates(["call_id"])

        assert df_dedup.count() == len(sample_cdr_data)

    def test_data_quality_rules(self, spark_session, sample_cdr_data):
        """Test data quality validation rules"""
        df = spark_session.createDataFrame(sample_cdr_data)

        # Test: Duration should be non-negative
        invalid_duration = df.filter(df.duration_seconds < 0).count()
        assert invalid_duration == 0

        # Test: Data volume should be non-negative
        invalid_data = df.filter(df.data_volume_mb < 0).count()
        assert invalid_data == 0

        # Test: Call type should be valid
        valid_types = {"voice", "sms", "data"}
        from pyspark.sql import functions as F

        invalid_type = df.filter(~F.col("call_type").isin(valid_types)).count()
        assert invalid_type == 0

    def test_aggregation_logic(self, spark_session, sample_cdr_data):
        """Test daily aggregation calculations"""
        df = spark_session.createDataFrame(sample_cdr_data)

        from pyspark.sql import functions as F

        # Aggregate by customer
        df_agg = df.groupBy("customer_id").agg(
            F.count("call_id").alias("total_calls"),
            F.sum("duration_seconds").alias("total_duration"),
            F.sum("data_volume_mb").alias("total_data"),
            F.sum("call_cost").alias("total_cost"),
        )

        # Verify aggregations
        assert df_agg.count() == len(sample_cdr_data["customer_id"].unique())

        # Check that total calls per customer is correct
        sample_customer = df_agg.filter(df_agg.customer_id == "CUST-0001").first()
        expected_calls = len(
            sample_cdr_data[sample_cdr_data["customer_id"] == "CUST-0001"]
        )
        assert sample_customer["total_calls"] == expected_calls


# ========================================
# INTEGRATION TESTS - END-TO-END PIPELINE
# ========================================


class TestPipelineIntegration:
    """Test complete pipeline workflows"""

    @pytest.mark.integration
    def test_bronze_to_silver_pipeline(self, spark_session, tmp_path, sample_cdr_data):
        """Test Bronze to Silver layer transformation"""
        # Write test data to Bronze
        bronze_path = str(tmp_path / "bronze")
        df_bronze = spark_session.createDataFrame(sample_cdr_data)
        df_bronze.write.format("parquet").mode("overwrite").save(bronze_path)

        # Apply Silver transformations
        from pyspark.sql import functions as F

        df_silver = spark_session.read.parquet(bronze_path)
        df_silver = (
            df_silver.filter(F.col("call_id").isNotNull())
            .filter(F.col("duration_seconds") >= 0)
            .withColumn("call_date", F.to_date("call_start_time"))
        )

        # Write to Silver
        silver_path = str(tmp_path / "silver")
        df_silver.write.format("parquet").mode("overwrite").partitionBy(
            "call_date"
        ).save(silver_path)

        # Verify Silver layer
        df_verify = spark_session.read.parquet(silver_path)
        assert df_verify.count() > 0
        assert "call_date" in df_verify.columns

    @pytest.mark.integration
    def test_gold_layer_aggregation(self, spark_session, tmp_path, sample_cdr_data):
        """Test Gold layer aggregation creation"""
        from pyspark.sql import functions as F

        # Create Silver data
        df_silver = spark_session.createDataFrame(sample_cdr_data)
        df_silver = df_silver.withColumn("call_date", F.to_date("call_start_time"))

        # Create Gold aggregation
        df_gold = df_silver.groupBy("customer_id", "call_date").agg(
            F.count("call_id").alias("total_calls"),
            F.sum("duration_seconds").alias("total_duration"),
            F.sum("data_volume_mb").alias("total_data"),
            F.avg("signal_strength_dbm").alias("avg_signal_strength"),
        )

        # Write to Gold
        gold_path = str(tmp_path / "gold")
        df_gold.write.format("parquet").mode("overwrite").partitionBy("call_date").save(
            gold_path
        )

        # Verify Gold layer
        df_verify = spark_session.read.parquet(gold_path)
        assert df_verify.count() > 0
        assert all(
            col in df_verify.columns
            for col in ["customer_id", "total_calls", "total_data"]
        )


# ========================================
# STREAMING TESTS
# ========================================


class TestStreamingPipeline:
    """Test real-time streaming components"""

    @pytest.mark.streaming
    def test_kafka_producer(self):
        """Test Kafka event production"""
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        test_event = {
            "event_id": "TEST-001",
            "cell_id": "CELL-0001",
            "event_type": "normal",
            "event_timestamp": datetime.now().isoformat(),
            "bandwidth_utilization_percent": 50.0,
        }

        # Send test event
        future = producer.send("network-events-test", value=test_event)
        result = future.get(timeout=10)

        assert result is not None
        producer.close()

    @pytest.mark.streaming
    def test_kafka_consumer(self):
        """Test Kafka event consumption"""
        consumer = KafkaConsumer(
            "network-events-test",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= 1:
                break

        consumer.close()
        assert len(messages) > 0

    @pytest.mark.streaming
    def test_stream_windowing(self, spark_session):
        """Test streaming window aggregations"""
        from pyspark.sql import functions as F

        # Create sample streaming data
        data = [
            ("CELL-001", datetime.now(), 80.0, 2.5),
            ("CELL-001", datetime.now() + timedelta(minutes=1), 85.0, 3.0),
            ("CELL-002", datetime.now(), 60.0, 1.5),
        ]

        df = spark_session.createDataFrame(
            data, ["cell_id", "event_timestamp", "bandwidth_util", "error_rate"]
        )

        # Apply windowing logic
        df_windowed = df.groupBy(
            F.window("event_timestamp", "5 minutes"), "cell_id"
        ).agg(
            F.avg("bandwidth_util").alias("avg_bandwidth"),
            F.max("bandwidth_util").alias("max_bandwidth"),
        )

        assert df_windowed.count() > 0


# ========================================
# API TESTS
# ========================================


class TestAPIEndpoints:
    """Test REST API endpoints"""

    @pytest.mark.asyncio
    async def test_health_endpoint(self, api_client):
        """Test health check endpoint"""
        response = await api_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_authentication_required(self, api_client):
        """Test API key authentication"""
        # Request without API key
        response = await api_client.get("/api/v1/metrics/realtime")
        assert response.status_code == 401

        # Request with valid API key
        headers = {"X-API-Key": "test-api-key-12345"}
        response = await api_client.get("/api/v1/metrics/realtime", headers=headers)
        assert response.status_code in [200, 404]  # 404 if no data

    @pytest.mark.asyncio
    async def test_customer_usage_endpoint(self, api_client):
        """Test customer usage API"""
        headers = {"X-API-Key": "test-api-key-12345"}
        params = {"start_date": "2025-01-01", "end_date": "2025-01-31"}

        response = await api_client.get(
            "/api/v1/customers/CUST-0001/usage", headers=headers, params=params
        )

        # Should return 200 or 404 depending on data availability
        assert response.status_code in [200, 404]

    @pytest.mark.asyncio
    async def test_rate_limiting(self, api_client):
        """Test API rate limiting"""
        headers = {"X-API-Key": "test-api-key-12345"}

        # Make multiple rapid requests
        responses = []
        for _ in range(150):
            response = await api_client.get("/api/v1/metrics/realtime", headers=headers)
            responses.append(response.status_code)

        # Should hit rate limit (429 Too Many Requests)
        assert 429 in responses


# ========================================
# LOAD/PERFORMANCE TESTS
# ========================================


class TestPerformance:
    """Performance and load testing"""

    @pytest.mark.performance
    def test_large_dataset_processing(self, spark_session):
        """Test processing of large dataset"""
        # Generate large dataset (1M records)
        from pyspark.sql import functions as F

        df_large = spark_session.range(0, 1000000).select(
            F.concat(F.lit("CALL-"), F.col("id").cast("string")).alias("call_id"),
            F.concat(F.lit("CUST-"), (F.col("id") % 10000).cast("string")).alias(
                "customer_id"
            ),
            (F.col("id") % 3).cast("int").alias("call_type_id"),
            (F.rand() * 3600).cast("int").alias("duration_seconds"),
            (F.rand() * 1000).alias("data_volume_mb"),
        )

        # Time the aggregation
        start_time = time.time()
        result = (
            df_large.groupBy("customer_id")
            .agg(
                F.count("call_id").alias("total_calls"),
                F.sum("data_volume_mb").alias("total_data"),
            )
            .collect()
        )

        elapsed_time = time.time() - start_time

        # Should process 1M records in reasonable time (< 30 seconds)
        assert elapsed_time < 30
        assert len(result) == 10000

    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_api_concurrent_requests(self, api_client):
        """Test API performance under concurrent load"""
        headers = {"X-API-Key": "test-api-key-12345"}

        async def make_request():
            return await api_client.get("/health", headers=headers)

        # Make 100 concurrent requests
        start_time = time.time()
        tasks = [make_request() for _ in range(100)]
        responses = await asyncio.gather(*tasks)
        elapsed_time = time.time() - start_time

        # All requests should succeed
        success_count = sum(1 for r in responses if r.status_code == 200)
        assert success_count == 100

        # Should handle 100 requests in < 5 seconds
        assert elapsed_time < 5

        # Calculate throughput
        throughput = 100 / elapsed_time
        print(f"API Throughput: {throughput:.2f} req/sec")
        assert throughput > 20  # At least 20 req/sec


# ========================================
# DATA QUALITY TESTS
# ========================================


class TestDataQuality:
    """Data quality validation tests"""

    def test_completeness_check(self, spark_session, sample_cdr_data):
        """Test data completeness"""
        df = spark_session.createDataFrame(sample_cdr_data)
        total_records = df.count()

        # Check completeness of critical fields
        critical_fields = ["call_id", "customer_id", "call_start_time"]

        for field in critical_fields:
            non_null_count = df.filter(df[field].isNotNull()).count()
            completeness_rate = (non_null_count / total_records) * 100

            # Should have 100% completeness for critical fields
            assert (
                completeness_rate == 100.0
            ), f"{field} completeness: {completeness_rate}%"

    def test_consistency_check(self, spark_session, sample_cdr_data):
        """Test data consistency rules"""
        df = spark_session.createDataFrame(sample_cdr_data)

        # Rule: call_end_time should be after call_start_time
        from pyspark.sql import functions as F

        inconsistent = df.filter(
            F.col("call_end_time") < F.col("call_start_time")
        ).count()
        assert inconsistent == 0, f"Found {inconsistent} inconsistent time records"

        # Rule: duration should match time difference
        df_with_calc = df.withColumn(
            "calculated_duration",
            (F.unix_timestamp("call_end_time") - F.unix_timestamp("call_start_time")),
        )
        # Allow 1 second tolerance
        mismatched = df_with_calc.filter(
            F.abs(F.col("duration_seconds") - F.col("calculated_duration")) > 1
        ).count()

        mismatch_rate = (mismatched / df.count()) * 100
        assert mismatch_rate < 1, f"Duration mismatch rate: {mismatch_rate}%"

    def test_accuracy_check(self, spark_session, sample_cdr_data):
        """Test data accuracy"""
        df = spark_session.createDataFrame(sample_cdr_data)

        # Check valid ranges
        from pyspark.sql import functions as F

        # Signal strength should be between -120 and -30 dBm
        invalid_signal = df.filter(
            (F.col("signal_strength_dbm") < -120) | (F.col("signal_strength_dbm") > -30)
        ).count()
        assert invalid_signal == 0

        # Data volume should be reasonable (< 10GB per call)
        invalid_data = df.filter(F.col("data_volume_mb") > 10000).count()
        assert invalid_data == 0

    def test_uniqueness_check(self, spark_session, sample_cdr_data):
        """Test uniqueness constraints"""
        df = spark_session.createDataFrame(sample_cdr_data)

        # call_id should be unique
        total_records = df.count()
        unique_call_ids = df.select("call_id").distinct().count()

        assert total_records == unique_call_ids, "call_id is not unique"


# ========================================
# SCHEMA EVOLUTION TESTS
# ========================================


class TestSchemaEvolution:
    """Test schema evolution and backward compatibility"""

    def test_add_new_column(self, spark_session, sample_cdr_data, tmp_path):
        """Test adding new column to schema"""
        from pyspark.sql import functions as F

        # Write initial data
        df_v1 = spark_session.createDataFrame(sample_cdr_data)
        path = str(tmp_path / "evolution_test")
        df_v1.write.format("parquet").mode("overwrite").save(path)

        # Add new column
        df_v2 = df_v1.withColumn("roaming_flag", F.lit(False))
        df_v2.write.format("parquet").mode("append").save(path)

        # Read back and verify
        df_read = spark_session.read.parquet(path)
        assert "roaming_flag" in df_read.columns

        # Old records should have null/default for new column
        old_records = df_read.limit(len(sample_cdr_data))
        assert old_records.filter(F.col("roaming_flag").isNull()).count() > 0

    def test_backward_compatibility(self, spark_session, sample_cdr_data):
        """Test reading old schema with new code"""
        # Simulate old schema (missing some columns)
        df_old = spark_session.createDataFrame(
            sample_cdr_data[["call_id", "customer_id", "call_type"]]
        )

        # New code expects additional columns - should handle gracefully
        from pyspark.sql import functions as F

        # Add missing columns with defaults
        df_compatible = df_old.withColumn("duration_seconds", F.lit(0)).withColumn(
            "data_volume_mb", F.lit(0.0)
        )

        assert df_compatible.count() == len(sample_cdr_data)


# ========================================
# ERROR HANDLING TESTS
# ========================================


class TestErrorHandling:
    """Test error handling and recovery"""

    def test_malformed_data_handling(self, spark_session):
        """Test handling of malformed data"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        # Create data with type mismatches
        malformed_data = [
            ("CALL-001", "CUST-001", "voice", "300"),  # duration as string
            ("CALL-002", "CUST-002", "sms", "invalid"),  # invalid duration
            ("CALL-003", "CUST-003", "data", "450"),
        ]

        schema = StructType(
            [
                StructField("call_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("call_type", StringType(), False),
                StructField("duration_seconds", StringType(), True),
            ]
        )

        df = spark_session.createDataFrame(malformed_data, schema)

        # Cast with error handling
        from pyspark.sql import functions as F

        df_cleaned = df.withColumn(
            "duration_seconds",
            F.when(
                F.col("duration_seconds").cast("int").isNotNull(),
                F.col("duration_seconds").cast("int"),
            ).otherwise(0),
        )

        # Should not fail, invalid values converted to 0
        assert df_cleaned.filter(F.col("duration_seconds") == 0).count() == 1

    @pytest.mark.asyncio
    async def test_api_error_responses(self, api_client):
        """Test API error handling"""
        headers = {"X-API-Key": "test-api-key-12345"}

        # Test 404 - Resource not found
        response = await api_client.get(
            "/api/v1/customers/INVALID-ID/usage",
            headers=headers,
            params={"start_date": "2025-01-01", "end_date": "2025-01-31"},
        )
        assert response.status_code == 404
        assert "error" in response.json()

        # Test 400 - Invalid parameters
        response = await api_client.get(
            "/api/v1/customers/CUST-001/usage",
            headers=headers,
            params={
                "start_date": "2025-01-31",
                "end_date": "2025-01-01",
            },  # end before start
        )
        assert response.status_code == 422  # Validation error


# ========================================
# PYTEST CONFIGURATION
# ========================================


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment"""
    import os

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["TESTING"] = "true"

    yield

    # Cleanup
    import shutil

    if os.path.exists("/tmp/test_data"):
        shutil.rmtree("/tmp/test_data")


def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "streaming: Streaming tests")
    config.addinivalue_line("markers", "performance: Performance tests")
    config.addinivalue_line("markers", "slow: Slow running tests")


# ========================================
# TEST RUNNER
# ========================================

if __name__ == "__main__":
    pytest.main(
        [
            __file__,
            "-v",
            "--tb=short",
            "--junit-xml=test-results.xml",
            "--cov=processing",
            "--cov-report=html",
            "--cov-report=term",
        ]
    )
