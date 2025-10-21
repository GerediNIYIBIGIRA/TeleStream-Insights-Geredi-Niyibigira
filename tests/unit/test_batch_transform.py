# # # """
# # # Unit tests for batch_transform.normalize_columns and enrich_with_hash using pytest.
# # # """
# # # import pandas as pd
# # # from processing.batch_transform import normalize_columns, enrich_with_hash


# # # def test_normalize_columns():
# # #     df = pd.DataFrame({"A Col": [1], " Another": [2]})
# # #     out = normalize_columns(df)
# # #     assert "a_col" in out.columns
# # #     assert "another" in out.columns


# # # def test_enrich_with_hash():
# # #     df = pd.DataFrame({"id": [1], "value": ["x"]})
# # #     out = enrich_with_hash(df, ["id", "value"])
# # #     assert "record_hash" in out.columns
# # #     assert out["record_hash"].iloc[0] is not None


# # """
# # Unit tests for batch transformation pipeline
# # """

# # import pytest
# # from pyspark.sql import SparkSession
# # from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
# # from datetime import datetime
# # from processing.batch_transform import TelestreamETLPipeline


# # @pytest.fixture(scope="module")
# # def spark():
# #     """Create Spark session for testing"""
# #     spark = SparkSession.builder \
# #         .master("local[2]") \
# #         .appName("test") \
# #         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# #         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
# #         .getOrCreate()

# #     yield spark
# #     spark.stop()


# # @pytest.fixture
# # def pipeline(spark):
# #     """Create pipeline instance for testing"""
# #     return TelestreamETLPipeline(environment="dev")


# # @pytest.fixture
# # def sample_cdr_data(spark):
# #     """Create sample CDR data for testing"""
# #     schema = StructType([
# #         StructField("call_id", StringType(), False),
# #         StructField("customer_id", StringType(), False),
# #         StructField("call_type", StringType(), False),
# #         StructField("call_direction", StringType(), True),
# #         StructField("call_start_time", TimestampType(), False),
# #         StructField("call_end_time", TimestampType(), True),
# #         StructField("duration_seconds", IntegerType(), True),
# #         StructField("originating_cell_id", StringType(), True),
# #         StructField("terminating_cell_id", StringType(), True),
# #         StructField("data_volume_mb", DoubleType(), True),
# #         StructField("sms_count", IntegerType(), True),
# #         StructField("signal_strength_dbm", IntegerType(), True),
# #         StructField("packet_loss_percent", DoubleType(), True),
# #         StructField("call_cost", DoubleType(), True),
# #         StructField("roaming_flag", BooleanType(), True),
# #     ])

# #     data = [
# #         ("CALL001", "CUST001", "voice", "outgoing", datetime(2024, 1, 15, 10, 30),
# #          datetime(2024, 1, 15, 10, 35), 300, "CELL001", "CELL002", 0.0, 0, -75, 0.5, 2.5, False),
# #         ("CALL002", "CUST002", "data", "outgoing", datetime(2024, 1, 15, 11, 0),
# #          datetime(2024, 1, 15, 11, 10), 600, "CELL001", "CELL001", 50.5, 0, -80, 1.2, 5.0, False),
# #         ("CALL003", "CUST001", "voice", "incoming", datetime(2024, 1, 15, 14, 0),
# #          datetime(2024, 1, 15, 14, 0), 15, "CELL003", "CELL001", 0.0, 0, -90, 3.5, 0.5, False),
# #     ]

# #     return spark.createDataFrame(data, schema)


# # class TestDataQuality:
# #     """Test data quality checks"""

# #     def test_null_filtering(self, pipeline, sample_cdr_data):
# #         """Test that null required fields are filtered"""
# #         # Add a row with null call_id
# #         from pyspark.sql import Row
# #         null_row = Row(call_id=None, customer_id="CUST003", call_type="voice",
# #                       call_direction="outgoing", call_start_time=datetime.now(),
# #                       call_end_time=None, duration_seconds=100, originating_cell_id="CELL001",
# #                       terminating_cell_id="CELL002", data_volume_mb=0.0, sms_count=0,
# #                       signal_strength_dbm=-75, packet_loss_percent=0.5, call_cost=1.0,
# #                       roaming_flag=False)

# #         df_with_null = sample_cdr_data.union(pipeline.spark.createDataFrame([null_row]))

# #         # Filter nulls (simulating the Silver layer logic)
# #         df_clean = df_with_null \
# #             .filter(df_with_null.call_id.isNotNull()) \
# #             .filter(df_with_null.customer_id.isNotNull()) \
# #             .filter(df_with_null.call_start_time.isNotNull())

# #         assert df_clean.count() == sample_cdr_data.count()

# #     def test_negative_value_filtering(self, pipeline, sample_cdr_data):
# #         """Test that negative values are filtered"""
# #         from pyspark.sql import functions as F

# #         # Add row with negative duration
# #         negative_row = sample_cdr_data.first()
# #         df_with_negative = sample_cdr_data.withColumn("duration_seconds", F.lit(-100))

# #         df_clean = df_with_negative \
# #             .filter(F.col("duration_seconds") >= 0)

# #         assert df_clean.count() == 0


# # class TestBusinessLogic:
# #     """Test business logic transformations"""

# #     def test_call_duration_minutes_calculation(self, pipeline, sample_cdr_data):
# #         """Test call duration conversion to minutes"""
# #         from pyspark.sql import functions as F

# #         df_transformed = sample_cdr_data.withColumn(
# #             "call_duration_minutes",
# #             F.round(F.col("duration_seconds") / 60, 2)
# #         )

# #         result = df_transformed.select("duration_seconds", "call_duration_minutes").collect()

# #         assert result[0]["call_duration_minutes"] == 5.0
# #         assert result[1]["call_duration_minutes"] == 10.0

# #     def test_short_call_detection(self, pipeline, sample_cdr_data):
# #         """Test short call flag"""
# #         from pyspark.sql import functions as F

# #         df_transformed = sample_cdr_data.withColumn(
# #             "is_short_call",
# #             F.when(F.col("duration_seconds") < 30, True).otherwise(False)
# #         )

# #         short_calls = df_transformed.filter(F.col("is_short_call")).count()
# #         assert short_calls == 1  # Only CALL003 has duration < 30

# #     def test_dropped_call_detection(self, pipeline, sample_cdr_data):
# #         """Test dropped call detection logic"""
# #         from pyspark.sql import functions as F

# #         df_transformed = sample_cdr_data.withColumn(
# #             "is_dropped_call",
# #             F.when((F.col("duration_seconds") < 30) & (F.col("call_type") == "voice"), True)
# #             .otherwise(False)
# #         )

# #         dropped_calls = df_transformed.filter(F.col("is_dropped_call")).count()
# #         assert dropped_calls == 1  # CALL003 is voice call < 30 seconds

# #     def test_revenue_categorization(self, pipeline, sample_cdr_data):
# #         """Test revenue category assignment"""
# #         from pyspark.sql import functions as F

# #         df_transformed = sample_cdr_data.withColumn(
# #             "revenue_category",
# #             F.when(F.col("call_cost") > 10, "high")
# #             .when(F.col("call_cost") > 5, "medium")
# #             .otherwise("low")
# #         )

# #         categories = df_transformed.select("call_cost", "revenue_category").collect()

# #         assert categories[0]["revenue_category"] == "low"   # 2.5
# #         assert categories[1]["revenue_category"] == "low"   # 5.0 (not > 5)
# #         assert categories[2]["revenue_category"] == "low"   # 0.5

# #     def test_signal_quality_classification(self, pipeline, sample_cdr_data):
# #         """Test signal quality classification"""
# #         from pyspark.sql import functions as F

# #         df_transformed = sample_cdr_data.withColumn(
# #             "signal_quality",
# #             F.when(F.col("signal_strength_dbm") > -70, "excellent")
# #             .when(F.col("signal_strength_dbm") > -85, "good")
# #             .when(F.col("signal_strength_dbm") > -100, "fair")
# #             .otherwise("poor")
# #         )

# #         qualities = df_transformed.select("signal_strength_dbm", "signal_quality").collect()

# #         assert qualities[0]["signal_quality"] == "good"       # -75
# #         assert qualities[1]["signal_quality"] == "good"       # -80
# #         assert qualities[2]["signal_quality"] == "fair"       # -90


# # class TestAggregations:
# #     """Test aggregation logic"""

# #     def test_daily_summary_aggregation(self, pipeline, sample_cdr_data):
# #         """Test daily usage summary aggregation"""
# #         from pyspark.sql import functions as F

# #         df_agg = sample_cdr_data.groupBy("customer_id") \
# #             .agg(
# #                 F.count("call_id").alias("total_calls"),
# #                 F.sum("duration_seconds").alias("total_duration_seconds"),
# #                 F.sum("data_volume_mb").alias("total_data_mb"),
# #                 F.sum("call_cost").alias("total_cost")
# #             )

# #         cust001 = df_agg.filter(F.col("customer_id") == "CUST001").collect()[0]

# #         assert cust001["total_calls"] == 2
# #         assert cust001["total_duration_seconds"] == 315  # 300 + 15
# #         assert cust001["total_cost"] == 3.0  # 2.5 + 0.5

# #     def test_network_performance_aggregation(self, pipeline, sample_cdr_data):
# #         """Test network performance metrics by cell"""
# #         from pyspark.sql import functions as F

# #         df_agg = sample_cdr_data.groupBy("originating_cell_id") \
# #             .agg(
# #                 F.count("call_id").alias("total_calls"),
# #                 F.countDistinct("customer_id").alias("unique_customers"),
# #                 F.avg("signal_strength_dbm").alias("avg_signal_strength"),
# #                 F.sum("data_volume_mb").alias("total_data_mb")
# #             )

# #         cell001 = df_agg.filter(F.col("originating_cell_id") == "CELL001").collect()[0]

# #         assert cell001["total_calls"] == 2
# #         assert cell001["unique_customers"] == 2


# # class TestEnrichment:
# #     """Test data enrichment functions"""

# #     def test_customer_enrichment(self, pipeline, sample_cdr_data):
# #         """Test customer data enrichment"""
# #         df_enriched = pipeline._enrich_with_customer_data(sample_cdr_data)

# #         assert "customer_segment" in df_enriched.columns
# #         assert df_enriched.count() == sample_cdr_data.count()


# # class TestDataQualityMetrics:
# #     """Test data quality metric calculations"""

# #     def test_dq_score_calculation(self, pipeline, sample_cdr_data):
# #         """Test data quality score calculation"""
# #         from pyspark.sql import functions as F

# #         df_with_dq = sample_cdr_data.withColumn(
# #             "dq_score",
# #             F.when(F.col("signal_strength_dbm").isNotNull(), 1).otherwise(0) +
# #             F.when(F.col("packet_loss_percent").isNotNull(), 1).otherwise(0) +
# #             F.when(F.col("data_volume_mb").isNotNull(), 1).otherwise(0)
# #         )

# #         scores = df_with_dq.select("dq_score").collect()

# #         # All test records have these three fields populated
# #         assert all(score["dq_score"] == 3 for score in scores)


# # if __name__ == "__main__":
# #     pytest.main([__file__, "-v"])


# """
# Unit tests for batch transformation pipeline
# Simplified version without PySpark session creation issues
# """

# import pytest
# from unittest.mock import Mock, MagicMock, patch
# from datetime import datetime
# from processing.batch_transform import TelestreamETLPipeline


# class TestTelestreamETLPipeline:
#     """Test the ETL pipeline class"""

#     @patch('processing.batch_transform.configure_spark_with_delta_pip')
#     @patch('processing.batch_transform.SparkSession')
#     def test_pipeline_initialization(self, mock_spark_session, mock_delta_config):
#         """Test pipeline initializes correctly"""
#         mock_spark = MagicMock()
#         mock_delta_config.return_value.getOrCreate.return_value = mock_spark

#         pipeline = TelestreamETLPipeline(environment="dev")

#         assert pipeline.environment == "dev"
#         assert pipeline.data_lake_path == "/tmp/datalake"
#         assert pipeline.spark is not None

#     @patch('processing.batch_transform.configure_spark_with_delta_pip')
#     @patch('processing.batch_transform.SparkSession')
#     def test_pipeline_prod_environment(self, mock_spark_session, mock_delta_config):
#         """Test pipeline uses correct path for production"""
#         mock_spark = MagicMock()
#         mock_delta_config.return_value.getOrCreate.return_value = mock_spark

#         pipeline = TelestreamETLPipeline(environment="prod")

#         assert pipeline.environment == "prod"
#         assert pipeline.data_lake_path == "s3a://telestream-datalake"


# # Alternative: Use actual PySpark with simpler configuration
# class TestBusinessLogicWithRealSpark:
#     """Tests using simplified PySpark configuration"""

#     @pytest.fixture(scope="class")
#     def spark(self):
#         """Create minimal Spark session for testing"""
#         try:
#             from pyspark.sql import SparkSession

#             spark = SparkSession.builder \
#                 .master("local[1]") \
#                 .appName("test") \
#                 .config("spark.ui.enabled", "false") \
#                 .config("spark.sql.shuffle.partitions", "1") \
#                 .getOrCreate()

#             yield spark
#             spark.stop()
#         except Exception as e:
#             pytest.skip(f"Could not create Spark session: {e}")

#     def test_business_transformations(self, spark):
#         """Test business logic transformations"""
#         from pyspark.sql import functions as F
#         from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

#         # Create simple test data
#         schema = StructType([
#             StructField("call_id", StringType(), False),
#             StructField("duration_seconds", IntegerType(), True),
#             StructField("call_cost", DoubleType(), True),
#             StructField("signal_strength_dbm", IntegerType(), True),
#         ])

#         data = [
#             ("CALL001", 300, 2.5, -75),
#             ("CALL002", 600, 12.0, -80),
#             ("CALL003", 15, 0.5, -90),
#         ]

#         df = spark.createDataFrame(data, schema)

#         # Test duration conversion
#         df_transformed = df.withColumn(
#             "call_duration_minutes",
#             F.round(F.col("duration_seconds") / 60, 2)
#         )

#         result = df_transformed.select("duration_seconds", "call_duration_minutes").collect()
#         assert result[0]["call_duration_minutes"] == 5.0

#         # Test revenue categorization
#         df_revenue = df.withColumn(
#             "revenue_category",
#             F.when(F.col("call_cost") > 10, "high")
#             .when(F.col("call_cost") > 5, "medium")
#             .otherwise("low")
#         )

#         categories = df_revenue.select("revenue_category").collect()
#         assert categories[0]["revenue_category"] == "low"
#         assert categories[1]["revenue_category"] == "high"

#         # Test signal quality
#         df_signal = df.withColumn(
#             "signal_quality",
#             F.when(F.col("signal_strength_dbm") > -70, "excellent")
#             .when(F.col("signal_strength_dbm") > -85, "good")
#             .when(F.col("signal_strength_dbm") > -100, "fair")
#             .otherwise("poor")
#         )

#         qualities = df_signal.select("signal_quality").collect()
#         assert qualities[0]["signal_quality"] == "good"


# # Unit tests without Spark - Testing logic only
# class TestTransformationLogic:
#     """Test transformation logic without Spark dependencies"""

#     def test_revenue_category_logic(self):
#         """Test revenue categorization logic"""
#         def categorize_revenue(cost):
#             if cost > 10:
#                 return "high"
#             elif cost > 5:
#                 return "medium"
#             else:
#                 return "low"

#         assert categorize_revenue(2.5) == "low"
#         assert categorize_revenue(12.0) == "high"
#         assert categorize_revenue(7.5) == "medium"
#         assert categorize_revenue(5.0) == "low"  # Edge case

#     def test_signal_quality_logic(self):
#         """Test signal quality classification logic"""
#         def classify_signal(dbm):
#             if dbm > -70:
#                 return "excellent"
#             elif dbm > -85:
#                 return "good"
#             elif dbm > -100:
#                 return "fair"
#             else:
#                 return "poor"

#         assert classify_signal(-75) == "good"
#         assert classify_signal(-65) == "excellent"
#         assert classify_signal(-90) == "fair"
#         assert classify_signal(-105) == "poor"

#     def test_short_call_detection(self):
#         """Test short call detection logic"""
#         def is_short_call(duration_seconds):
#             return duration_seconds < 30

#         assert is_short_call(15) is True
#         assert is_short_call(300) is False
#         assert is_short_call(30) is False  # Edge case

#     def test_dropped_call_detection(self):
#         """Test dropped call detection logic"""
#         def is_dropped(duration_seconds, call_type):
#             return duration_seconds < 30 and call_type == "voice"

#         assert is_dropped(15, "voice") is True
#         assert is_dropped(15, "data") is False
#         assert is_dropped(300, "voice") is False

#     def test_duration_conversion(self):
#         """Test duration seconds to minutes conversion"""
#         def seconds_to_minutes(seconds):
#             return round(seconds / 60, 2)

#         assert seconds_to_minutes(300) == 5.0
#         assert seconds_to_minutes(600) == 10.0
#         assert seconds_to_minutes(90) == 1.5

#     def test_call_drop_rate_calculation(self):
#         """Test call drop rate calculation"""
#         def calculate_drop_rate(dropped_calls, total_calls):
#             if total_calls == 0:
#                 return 0.0
#             return round(dropped_calls / total_calls * 100, 2)

#         assert calculate_drop_rate(5, 100) == 5.0
#         assert calculate_drop_rate(1, 10) == 10.0
#         assert calculate_drop_rate(0, 100) == 0.0

#     def test_congestion_score_logic(self):
#         """Test congestion score calculation"""
#         def calculate_congestion(total_calls):
#             if total_calls > 1000:
#                 return 3
#             elif total_calls > 500:
#                 return 2
#             else:
#                 return 1

#         assert calculate_congestion(1500) == 3
#         assert calculate_congestion(750) == 2
#         assert calculate_congestion(300) == 1
#         assert calculate_congestion(1000) == 2  # Edge case


# class TestDataQualityLogic:
#     """Test data quality scoring logic"""

#     def test_dq_score_calculation(self):
#         """Test data quality score calculation"""
#         def calculate_dq_score(signal_strength, packet_loss, data_volume):
#             score = 0
#             if signal_strength is not None:
#                 score += 1
#             if packet_loss is not None:
#                 score += 1
#             if data_volume is not None:
#                 score += 1
#             return score

#         assert calculate_dq_score(-75, 0.5, 50.5) == 3
#         assert calculate_dq_score(-75, None, 50.5) == 2
#         assert calculate_dq_score(None, None, None) == 0


# class TestAggregationLogic:
#     """Test aggregation calculations"""

#     def test_average_call_duration(self):
#         """Test average call duration calculation"""
#         def avg_duration(total_duration, total_calls):
#             if total_calls == 0:
#                 return 0.0
#             return round(total_duration / total_calls, 2)

#         assert avg_duration(315, 2) == 157.5
#         assert avg_duration(1000, 10) == 100.0
#         assert avg_duration(0, 0) == 0.0


# if __name__ == "__main__":
#     pytest.main([__file__, "-v"])


######################################################## simple test file below ########################################################

"""
Unit tests for batch transformation pipeline
Tests pure transformation functions
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from pyspark.sql import functions as F

# Import transformation functions
from processing.batch_transform import (
    normalize_columns,
    enrich_with_hash,
    add_metadata_columns,
    apply_data_quality_filters,
    add_temporal_features,
    add_call_classifications,
    add_revenue_category,
    add_signal_quality,
    calculate_data_quality_score,
    aggregate_daily_usage,
    aggregate_network_performance,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def sample_cdr_data(spark):
    """Create sample CDR data for testing"""
    schema = StructType(
        [
            StructField("call_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("call_type", StringType(), False),
            StructField("call_direction", StringType(), True),
            StructField("call_start_time", TimestampType(), False),
            StructField("call_end_time", TimestampType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("originating_cell_id", StringType(), True),
            StructField("terminating_cell_id", StringType(), True),
            StructField("data_volume_mb", DoubleType(), True),
            StructField("sms_count", IntegerType(), True),
            StructField("signal_strength_dbm", IntegerType(), True),
            StructField("packet_loss_percent", DoubleType(), True),
            StructField("call_cost", DoubleType(), True),
            StructField("roaming_flag", BooleanType(), True),
        ]
    )

    data = [
        (
            "CALL001",
            "CUST001",
            "voice",
            "outgoing",
            datetime(2024, 1, 15, 10, 30),
            datetime(2024, 1, 15, 10, 35),
            300,
            "CELL001",
            "CELL002",
            0.0,
            0,
            -75,
            0.5,
            2.5,
            False,
        ),
        (
            "CALL002",
            "CUST002",
            "data",
            "outgoing",
            datetime(2024, 1, 15, 11, 0),
            datetime(2024, 1, 15, 11, 10),
            600,
            "CELL001",
            "CELL001",
            50.5,
            0,
            -80,
            1.2,
            12.0,
            False,
        ),
        (
            "CALL003",
            "CUST001",
            "voice",
            "incoming",
            datetime(2024, 1, 15, 14, 0),
            datetime(2024, 1, 15, 14, 0),
            15,
            "CELL003",
            "CELL001",
            0.0,
            0,
            -90,
            3.5,
            0.5,
            False,
        ),
    ]

    return spark.createDataFrame(data, schema)


class TestNormalization:
    """Test data normalization functions"""

    def test_normalize_columns(self, spark):
        """Test column normalization"""
        data = [("  UPPER  ", "MixedCase"), ("lower", "CAPS")]
        df = spark.createDataFrame(data, ["col1", "col2"])

        result = normalize_columns(df, ["col1", "col2"])
        values = result.collect()

        assert values[0]["col1"] == "upper"
        assert values[0]["col2"] == "mixedcase"
        assert values[1]["col1"] == "lower"

    def test_enrich_with_hash(self, spark):
        """Test hash generation"""
        data = [("A", "B"), ("C", "D")]
        df = spark.createDataFrame(data, ["col1", "col2"])

        result = enrich_with_hash(df, ["col1", "col2"])

        assert "row_hash" in result.columns
        assert result.count() == 2
        # Hashes should be unique
        assert result.select("row_hash").distinct().count() == 2


class TestMetadata:
    """Test metadata enrichment"""

    def test_add_metadata_columns(self, spark, sample_cdr_data):
        """Test metadata column addition"""
        result = add_metadata_columns(sample_cdr_data, "BATCH001")

        assert "batch_id" in result.columns
        assert "ingestion_timestamp" in result.columns
        assert result.select("batch_id").first()[0] == "BATCH001"


class TestDataQuality:
    """Test data quality filters"""

    def test_apply_data_quality_filters_removes_nulls(self, spark):
        """Test that null required fields are filtered"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("call_start_time", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("data_volume_mb", DoubleType(), True),
            ]
        )

        data = [
            ("CALL001", "CUST001", datetime(2024, 1, 15, 10, 30), 300, 50.0),
            (None, "CUST002", datetime(2024, 1, 15, 11, 0), 600, 100.0),  # Null call_id
            (
                "CALL003",
                None,
                datetime(2024, 1, 15, 14, 0),
                15,
                25.0,
            ),  # Null customer_id
        ]

        df = spark.createDataFrame(data, schema)
        result = apply_data_quality_filters(df)

        assert result.count() == 1
        assert result.first()["call_id"] == "CALL001"

    def test_apply_data_quality_filters_removes_negatives(self, spark):
        """Test that negative values are filtered"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("call_start_time", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("data_volume_mb", DoubleType(), True),
            ]
        )

        data = [
            ("CALL001", "CUST001", datetime(2024, 1, 15, 10, 30), 300, 50.0),
            (
                "CALL002",
                "CUST002",
                datetime(2024, 1, 15, 11, 0),
                -100,
                100.0,
            ),  # Negative duration
            (
                "CALL003",
                "CUST003",
                datetime(2024, 1, 15, 14, 0),
                15,
                -25.0,
            ),  # Negative data volume
        ]

        df = spark.createDataFrame(data, schema)
        result = apply_data_quality_filters(df)

        assert result.count() == 1

    def test_apply_data_quality_filters_removes_duplicates(self, spark):
        """Test duplicate removal"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("call_start_time", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("data_volume_mb", DoubleType(), True),
            ]
        )

        data = [
            ("CALL001", "CUST001", datetime(2024, 1, 15, 10, 30), 300, 50.0),
            (
                "CALL001",
                "CUST001",
                datetime(2024, 1, 15, 10, 30),
                300,
                50.0,
            ),  # Duplicate
        ]

        df = spark.createDataFrame(data, schema)
        result = apply_data_quality_filters(df)

        assert result.count() == 1


class TestTemporalFeatures:
    """Test temporal feature extraction"""

    def test_add_temporal_features(self, sample_cdr_data):
        """Test temporal features are added correctly"""
        result = add_temporal_features(sample_cdr_data)

        assert "call_date" in result.columns
        assert "call_hour" in result.columns
        assert "call_day_of_week" in result.columns
        assert "is_weekend" in result.columns

        row = result.first()
        assert row["call_hour"] == 10
        # 2024-01-15 is a Monday (day 2 in Spark's dayofweek)
        assert row["call_day_of_week"] == 2


class TestCallClassifications:
    """Test call classification logic"""

    def test_add_call_classifications(self, sample_cdr_data):
        """Test call classifications"""
        result = add_call_classifications(sample_cdr_data)

        assert "call_duration_minutes" in result.columns
        assert "is_short_call" in result.columns
        assert "is_dropped_call" in result.columns

        rows = result.collect()

        # CALL001: 300 seconds = 5 minutes
        assert rows[0]["call_duration_minutes"] == 5.0
        assert rows[0]["is_short_call"] is False

        # CALL003: 15 seconds, voice call
        assert rows[2]["is_short_call"] is True
        assert rows[2]["is_dropped_call"] is True


class TestRevenueCategory:
    """Test revenue categorization"""

    def test_add_revenue_category(self, sample_cdr_data):
        """Test revenue category assignment"""
        result = add_revenue_category(sample_cdr_data)

        assert "revenue_category" in result.columns

        rows = result.collect()

        # CALL001: 2.5 -> low
        assert rows[0]["revenue_category"] == "low"
        # CALL002: 12.0 -> high
        assert rows[1]["revenue_category"] == "high"
        # CALL003: 0.5 -> low
        assert rows[2]["revenue_category"] == "low"

    def test_revenue_category_edge_cases(self, spark):
        """Test revenue category edge cases"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("call_cost", DoubleType(), True),
            ]
        )

        data = [
            ("C1", 5.0),  # Exactly 5 -> low (not > 5)
            ("C2", 5.01),  # Just above 5 -> medium
            ("C3", 10.0),  # Exactly 10 -> medium (not > 10)
            ("C4", 10.01),  # Just above 10 -> high
        ]

        df = spark.createDataFrame(data, schema)
        result = add_revenue_category(df)

        rows = result.collect()
        assert rows[0]["revenue_category"] == "low"
        assert rows[1]["revenue_category"] == "medium"
        assert rows[2]["revenue_category"] == "medium"
        assert rows[3]["revenue_category"] == "high"


class TestSignalQuality:
    """Test signal quality classification"""

    def test_add_signal_quality(self, sample_cdr_data):
        """Test signal quality classification"""
        result = add_signal_quality(sample_cdr_data)

        assert "signal_quality" in result.columns

        rows = result.collect()

        # CALL001: -75 dBm -> good
        assert rows[0]["signal_quality"] == "good"
        # CALL002: -80 dBm -> good
        assert rows[1]["signal_quality"] == "good"
        # CALL003: -90 dBm -> fair
        assert rows[2]["signal_quality"] == "fair"

    def test_signal_quality_all_categories(self, spark):
        """Test all signal quality categories"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("signal_strength_dbm", IntegerType(), True),
            ]
        )

        data = [
            ("C1", -65),  # excellent (> -70)
            ("C2", -75),  # good (> -85)
            ("C3", -90),  # fair (> -100)
            ("C4", -105),  # poor (<= -100)
        ]

        df = spark.createDataFrame(data, schema)
        result = add_signal_quality(df)

        rows = result.collect()
        assert rows[0]["signal_quality"] == "excellent"
        assert rows[1]["signal_quality"] == "good"
        assert rows[2]["signal_quality"] == "fair"
        assert rows[3]["signal_quality"] == "poor"


class TestDataQualityScore:
    """Test data quality score calculation"""

    def test_calculate_data_quality_score(self, sample_cdr_data):
        """Test DQ score calculation"""
        result = calculate_data_quality_score(sample_cdr_data)

        assert "dq_score" in result.columns

        # All test records have signal_strength, packet_loss, and data_volume
        rows = result.collect()
        for row in rows:
            assert row["dq_score"] == 3

    def test_data_quality_score_with_nulls(self, spark):
        """Test DQ score with missing values"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("signal_strength_dbm", IntegerType(), True),
                StructField("packet_loss_percent", DoubleType(), True),
                StructField("data_volume_mb", DoubleType(), True),
            ]
        )

        data = [
            ("C1", -75, 0.5, 50.0),  # All present = 3
            ("C2", None, 0.5, 50.0),  # Missing signal = 2
            ("C3", -75, None, 50.0),  # Missing packet_loss = 2
            ("C4", None, None, None),  # All missing = 0
        ]

        df = spark.createDataFrame(data, schema)
        result = calculate_data_quality_score(df)

        rows = result.collect()
        assert rows[0]["dq_score"] == 3
        assert rows[1]["dq_score"] == 2
        assert rows[2]["dq_score"] == 2
        assert rows[3]["dq_score"] == 0


class TestAggregations:
    """Test aggregation functions"""

    def test_aggregate_daily_usage(self, sample_cdr_data):
        """Test daily usage aggregation"""
        # Add required columns for aggregation
        df = add_call_classifications(sample_cdr_data)
        df = df.withColumn("call_date", F.to_date("call_start_time"))
        df = df.withColumn("customer_segment", F.lit("consumer"))

        result = aggregate_daily_usage(df)

        assert "total_calls" in result.columns
        assert "total_duration_seconds" in result.columns
        assert "avg_call_duration_seconds" in result.columns
        assert "call_drop_rate" in result.columns

        # CUST001 has 2 calls
        cust001 = result.filter(F.col("customer_id") == "CUST001").first()
        assert cust001["total_calls"] == 2
        assert cust001["total_duration_seconds"] == 315  # 300 + 15
        assert cust001["total_cost"] == 3.0  # 2.5 + 0.5

    def test_aggregate_network_performance(self, sample_cdr_data):
        """Test network performance aggregation"""
        df = add_call_classifications(sample_cdr_data)
        df = df.withColumn("call_date", F.to_date("call_start_time"))

        result = aggregate_network_performance(df)

        assert "total_calls" in result.columns
        assert "unique_customers" in result.columns
        assert "avg_signal_strength" in result.columns
        assert "drop_rate_percent" in result.columns
        assert "congestion_score" in result.columns

        # CELL001 has 2 calls
        cell001 = result.filter(F.col("originating_cell_id") == "CELL001").first()
        assert cell001["total_calls"] == 2
        assert cell001["unique_customers"] == 2

    def test_congestion_score_calculation(self, spark):
        """Test congestion score thresholds"""
        schema = StructType(
            [
                StructField("originating_cell_id", StringType(), True),
                StructField("call_date", StringType(), True),
                StructField("call_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("signal_strength_dbm", IntegerType(), True),
                StructField("packet_loss_percent", DoubleType(), True),
                StructField("data_volume_mb", DoubleType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("is_dropped_call", BooleanType(), True),
            ]
        )

        # Create data with different call volumes
        data = []
        # Low congestion cell (300 calls)
        for i in range(300):
            data.append(
                (
                    "CELL_LOW",
                    "2024-01-15",
                    f"C{i}",
                    f"CUST{i}",
                    -75,
                    0.5,
                    50.0,
                    300,
                    False,
                )
            )
        # Medium congestion cell (750 calls)
        for i in range(750):
            data.append(
                (
                    "CELL_MED",
                    "2024-01-15",
                    f"C{i+300}",
                    f"CUST{i}",
                    -75,
                    0.5,
                    50.0,
                    300,
                    False,
                )
            )
        # High congestion cell (1500 calls)
        for i in range(1500):
            data.append(
                (
                    "CELL_HIGH",
                    "2024-01-15",
                    f"C{i+1050}",
                    f"CUST{i}",
                    -75,
                    0.5,
                    50.0,
                    300,
                    False,
                )
            )

        df = spark.createDataFrame(data, schema)
        result = aggregate_network_performance(df)

        scores = {
            row["originating_cell_id"]: row["congestion_score"]
            for row in result.collect()
        }

        assert scores["CELL_LOW"] == 1  # < 500 calls
        assert scores["CELL_MED"] == 2  # 500-1000 calls
        assert scores["CELL_HIGH"] == 3  # > 1000 calls


class TestIntegration:
    """Integration tests for complete transformation pipeline"""

    def test_complete_silver_transformation(self, sample_cdr_data):
        """Test complete transformation from bronze to silver"""
        # Apply all transformations in sequence
        df = apply_data_quality_filters(sample_cdr_data)
        df = add_temporal_features(df)
        df = add_call_classifications(df)
        df = add_revenue_category(df)
        df = add_signal_quality(df)
        df = calculate_data_quality_score(df)

        # Verify all columns exist
        expected_cols = [
            "call_date",
            "call_hour",
            "call_day_of_week",
            "is_weekend",
            "call_duration_minutes",
            "is_short_call",
            "is_dropped_call",
            "revenue_category",
            "signal_quality",
            "dq_score",
        ]

        for col in expected_cols:
            assert col in df.columns

        # Verify data quality
        assert df.count() == 3  # All 3 records should pass filters

        # Verify transformations work together
        row = df.filter(F.col("call_id") == "CALL002").first()
        assert row["revenue_category"] == "high"
        assert row["signal_quality"] == "good"
        assert row["is_short_call"] is False
        assert row["dq_score"] == 3

    def test_end_to_end_aggregation_pipeline(self, sample_cdr_data):
        """Test end-to-end aggregation pipeline"""
        # Transform to silver
        df = apply_data_quality_filters(sample_cdr_data)
        df = add_temporal_features(df)
        df = add_call_classifications(df)
        df = df.withColumn("customer_segment", F.lit("consumer"))

        # Aggregate to gold
        result = aggregate_daily_usage(df)

        # Verify aggregation quality
        assert result.count() == 2  # 2 unique customers

        # Verify KPIs are calculated correctly
        for row in result.collect():
            assert row["total_calls"] > 0
            assert row["avg_call_duration_seconds"] is not None
            assert row["call_drop_rate"] is not None
            assert 0 <= row["call_drop_rate"] <= 100


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_empty_dataframe(self, spark):
        """Test functions handle empty dataframes"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("call_start_time", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("data_volume_mb", DoubleType(), True),
                StructField("call_cost", DoubleType(), True),
            ]
        )

        df = spark.createDataFrame([], schema)

        result = add_revenue_category(df)
        assert result.count() == 0
        assert "revenue_category" in result.columns

    def test_zero_values(self, spark):
        """Test handling of zero values"""
        schema = StructType(
            [
                StructField("call_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("call_start_time", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("data_volume_mb", DoubleType(), True),
            ]
        )

        data = [
            ("CALL001", "CUST001", datetime(2024, 1, 15, 10, 30), 0, 0.0),
        ]

        df = spark.createDataFrame(data, schema)
        result = apply_data_quality_filters(df)

        # Zero should be accepted (not negative)
        assert result.count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
