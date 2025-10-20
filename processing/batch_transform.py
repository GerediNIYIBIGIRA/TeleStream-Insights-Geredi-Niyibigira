"""
Advanced Batch Processing with Delta Lake
Implements Medallion Architecture: Bronze -> Silver -> Gold
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta import DeltaTable, configure_spark_with_delta_pip
import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TelestreamETLPipeline:
    """Production-grade ETL pipeline with Delta Lake"""
    
    def __init__(self, environment: str = "dev"):
        self.environment = environment
        self.spark = self._create_spark_session()
        self.data_lake_path = "s3a://telestream-datalake" if environment == "prod" else "/tmp/datalake"
        
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session with Delta Lake"""
        builder = (
            SparkSession.builder
            .appName(f"TeleStream-ETL-{self.environment}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.default.parallelism", "200")
            .config("spark.memory.fraction", "0.8")
        )
        
        # Add S3 configs for production
        if self.environment == "prod":
            builder = builder \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                       "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    # ========================================
    # BRONZE LAYER: RAW DATA INGESTION
    # ========================================
    
    def ingest_cdr_to_bronze(self, source_path: str, batch_id: str) -> None:
        """
        Ingest raw CDR data to Bronze layer with minimal transformation
        Implements deduplication and schema validation
        """
        logger.info(f"Starting CDR ingestion for batch {batch_id}")
        
        # Define strict schema for validation
        cdr_schema = StructType([
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
        ])
        
        # Read with schema enforcement
        df_raw = self.spark.read \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(cdr_schema) \
            .csv(source_path)
        
        # Add metadata columns
        df_bronze = df_raw \
            .withColumn("batch_id", F.lit(batch_id)) \
            .withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("source_file", F.input_file_name()) \
            .withColumn("row_hash", F.sha2(F.concat_ws("|", *df_raw.columns), 256))
        
        # Write to Bronze with deduplication
        bronze_path = f"{self.data_lake_path}/bronze/cdr"
        
        if DeltaTable.isDeltaTable(self.spark, bronze_path):
            # Merge with deduplication
            delta_table = DeltaTable.forPath(self.spark, bronze_path)
            delta_table.alias("target").merge(
                df_bronze.alias("source"),
                "target.call_id = source.call_id AND target.call_start_time = source.call_start_time"
            ).whenNotMatchedInsertAll().execute()
        else:
            # Initial write
            df_bronze.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("batch_id") \
                .option("overwriteSchema", "true") \
                .save(bronze_path)
        
        logger.info(f"Bronze ingestion completed. Records: {df_bronze.count()}")
    
    # ========================================
    # SILVER LAYER: CLEANED & VALIDATED DATA
    # ========================================
    
    def transform_bronze_to_silver(self, batch_date: str) -> None:
        """
        Apply business rules, data quality checks, and enrichment
        """
        logger.info(f"Starting Bronze to Silver transformation for {batch_date}")
        
        bronze_path = f"{self.data_lake_path}/bronze/cdr"
        silver_path = f"{self.data_lake_path}/silver/cdr"
        
        # Read from Bronze
        df_bronze = self.spark.read.format("delta").load(bronze_path)
        
        # Data Quality Rules
        df_clean = df_bronze \
            .filter(F.col("call_id").isNotNull()) \
            .filter(F.col("customer_id").isNotNull()) \
            .filter(F.col("call_start_time").isNotNull()) \
            .filter(F.col("duration_seconds") >= 0) \
            .filter(F.col("data_volume_mb") >= 0) \
            .dropDuplicates(["call_id"])
        
        # Business Logic Transformations
        df_silver = df_clean \
            .withColumn("call_date", F.to_date("call_start_time")) \
            .withColumn("call_hour", F.hour("call_start_time")) \
            .withColumn("call_day_of_week", F.dayofweek("call_start_time")) \
            .withColumn("is_weekend", F.when(F.col("call_day_of_week").isin([1, 7]), True).otherwise(False)) \
            .withColumn("call_duration_minutes", F.round(F.col("duration_seconds") / 60, 2)) \
            .withColumn("is_short_call", F.when(F.col("duration_seconds") < 30, True).otherwise(False)) \
            .withColumn("is_dropped_call", 
                       F.when((F.col("duration_seconds") < 30) & (F.col("call_type") == "voice"), True)
                       .otherwise(False)) \
            .withColumn("revenue_category",
                       F.when(F.col("call_cost") > 10, "high")
                       .when(F.col("call_cost") > 5, "medium")
                       .otherwise("low")) \
            .withColumn("signal_quality",
                       F.when(F.col("signal_strength_dbm") > -70, "excellent")
                       .when(F.col("signal_strength_dbm") > -85, "good")
                       .when(F.col("signal_strength_dbm") > -100, "fair")
                       .otherwise("poor"))
        
        # Enrich with customer segment (lookup from dim table)
        df_enriched = self._enrich_with_customer_data(df_silver)
        
        # Add data quality score
        df_enriched = df_enriched.withColumn(
            "dq_score",
            F.when(F.col("signal_strength_dbm").isNotNull(), 1).otherwise(0) +
            F.when(F.col("packet_loss_percent").isNotNull(), 1).otherwise(0) +
            F.when(F.col("data_volume_mb").isNotNull(), 1).otherwise(0)
        )
        
        # Write to Silver with CDC (Change Data Capture)
        df_enriched \
            .withColumn("silver_timestamp", F.current_timestamp()) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("call_date") \
            .option("mergeSchema", "true") \
            .save(silver_path)
        
        # Log data quality metrics
        self._log_data_quality_metrics(df_enriched, "cdr_silver")
        
        logger.info(f"Silver transformation completed")
    
    # ========================================
    # GOLD LAYER: BUSINESS-READY AGGREGATES
    # ========================================
    
    def create_gold_daily_usage_summary(self, target_date: str) -> None:
        """
        Create daily usage aggregates for analytics
        """
        logger.info(f"Creating Gold layer daily summary for {target_date}")
        
        silver_path = f"{self.data_lake_path}/silver/cdr"
        gold_path = f"{self.data_lake_path}/gold/daily_usage_summary"
        
        df_silver = self.spark.read.format("delta").load(silver_path) \
            .filter(F.col("call_date") == target_date)
        
        # Aggregate metrics by customer and date
        df_gold = df_silver.groupBy("customer_id", "call_date") \
            .agg(
                F.count("call_id").alias("total_calls"),
                F.sum("duration_seconds").alias("total_duration_seconds"),
                F.sum("data_volume_mb").alias("total_data_mb"),
                F.sum("sms_count").alias("total_sms_count"),
                F.sum("call_cost").alias("total_cost"),
                F.avg("signal_strength_dbm").alias("avg_signal_strength"),
                F.sum(F.when(F.col("is_dropped_call"), 1).otherwise(0)).alias("dropped_calls_count"),
                F.sum(F.when(F.col("roaming_flag"), F.col("duration_seconds")).otherwise(0))
                    .alias("roaming_duration_seconds"),
                F.countDistinct("originating_cell_id").alias("unique_cells_used"),
                F.max("call_start_time").alias("last_call_time"),
                F.first("customer_segment").alias("customer_segment")
            ) \
            .withColumn("avg_call_duration_seconds", 
                       F.round(F.col("total_duration_seconds") / F.col("total_calls"), 2)) \
            .withColumn("call_drop_rate", 
                       F.round(F.col("dropped_calls_count") / F.col("total_calls") * 100, 2)) \
            .withColumn("gold_timestamp", F.current_timestamp())
        
        # Write to Gold with upsert capability
        if DeltaTable.isDeltaTable(self.spark, gold_path):
            delta_table = DeltaTable.forPath(self.spark, gold_path)
            delta_table.alias("target").merge(
                df_gold.alias("source"),
                "target.customer_id = source.customer_id AND target.call_date = source.call_date"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            df_gold.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("call_date") \
                .save(gold_path)
        
        logger.info(f"Gold daily summary created: {df_gold.count()} records")
    
    def create_gold_network_performance(self, target_date: str) -> None:
        """
        Aggregate network performance metrics by cell tower
        """
        silver_path = f"{self.data_lake_path}/silver/cdr"
        gold_path = f"{self.data_lake_path}/gold/network_performance"
        
        df_silver = self.spark.read.format("delta").load(silver_path) \
            .filter(F.col("call_date") == target_date)
        
        # Network performance aggregation
        df_gold = df_silver.groupBy("originating_cell_id", "call_date") \
            .agg(
                F.count("call_id").alias("total_calls"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.avg("signal_strength_dbm").alias("avg_signal_strength"),
                F.avg("packet_loss_percent").alias("avg_packet_loss"),
                F.sum("data_volume_mb").alias("total_data_mb"),
                F.sum(F.when(F.col("is_dropped_call"), 1).otherwise(0)).alias("dropped_calls"),
                F.percentile_approx("duration_seconds", 0.5).alias("median_call_duration"),
                F.percentile_approx("duration_seconds", 0.95).alias("p95_call_duration")
            ) \
            .withColumn("drop_rate_percent", 
                       F.round(F.col("dropped_calls") / F.col("total_calls") * 100, 2)) \
            .withColumn("congestion_score",
                       F.when(F.col("total_calls") > 1000, 3)
                       .when(F.col("total_calls") > 500, 2)
                       .otherwise(1)) \
            .withColumn("gold_timestamp", F.current_timestamp())
        
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("call_date") \
            .save(gold_path)
    
    # ========================================
    # ADVANCED FEATURES
    # ========================================
    
    def detect_usage_anomalies(self, lookback_days: int = 30) -> DataFrame:
        """
        Real-time anomaly detection using statistical methods
        """
        gold_path = f"{self.data_lake_path}/gold/daily_usage_summary"
        
        df = self.spark.read.format("delta").load(gold_path)
        
        # Calculate rolling statistics per customer
        window_spec = Window.partitionBy("customer_id") \
            .orderBy("call_date") \
            .rowsBetween(-lookback_days, -1)
        
        df_with_stats = df \
            .withColumn("avg_data_30d", F.avg("total_data_mb").over(window_spec)) \
            .withColumn("stddev_data_30d", F.stddev("total_data_mb").over(window_spec)) \
            .withColumn("avg_cost_30d", F.avg("total_cost").over(window_spec)) \
            .withColumn("stddev_cost_30d", F.stddev("total_cost").over(window_spec))
        
        # Detect anomalies (> 3 standard deviations)
        df_anomalies = df_with_stats \
            .withColumn("data_zscore",
                       (F.col("total_data_mb") - F.col("avg_data_30d")) / F.col("stddev_data_30d")) \
            .withColumn("cost_zscore",
                       (F.col("total_cost") - F.col("avg_cost_30d")) / F.col("stddev_cost_30d")) \
            .withColumn("is_anomaly",
                       (F.abs(F.col("data_zscore")) > 3) | (F.abs(F.col("cost_zscore")) > 3)) \
            .filter(F.col("is_anomaly"))
        
        return df_anomalies
    
    def _enrich_with_customer_data(self, df: DataFrame) -> DataFrame:
        """Enrich with customer dimension data"""
        # In production, this would join with customer dimension from warehouse
        # For now, add mock customer segment
        return df.withColumn("customer_segment",
            F.when(F.rand() > 0.7, "enterprise")
            .when(F.rand() > 0.4, "consumer")
            .otherwise("SMB"))
    
    def _log_data_quality_metrics(self, df: DataFrame, table_name: str) -> None:
        """Log data quality metrics for observability"""
        total_records = df.count()
        null_counts = {col: df.filter(F.col(col).isNull()).count() 
                      for col in df.columns}
        
        quality_metrics = {
            "table": table_name,
            "timestamp": datetime.now().isoformat(),
            "total_records": total_records,
            "null_percentages": {k: v/total_records*100 for k, v in null_counts.items()},
            "duplicate_rate": (df.count() - df.dropDuplicates().count()) / total_records * 100
        }
        
        logger.info(f"Data Quality Metrics: {quality_metrics}")
    
    # ========================================
    # OPTIMIZATION & MAINTENANCE
    # ========================================
    
    def optimize_delta_tables(self, table_path: str) -> None:
        """Run optimization on Delta tables"""
        logger.info(f"Optimizing table: {table_path}")
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Z-ORDER optimization for frequently filtered columns
        delta_table.optimize().executeZOrderBy("customer_id", "call_date")
        
        # Vacuum old files (remove files older than 7 days)
        delta_table.vacuum(retentionHours=168)
        
        logger.info("Optimization completed")
    
    def generate_time_travel_report(self, table_path: str, version: int) -> DataFrame:
        """Time travel capability - read historical version"""
        return self.spark.read \
            .format("delta") \
            .option("versionAsOf", version) \
            .load(table_path)


# ========================================
# MAIN EXECUTION
# ========================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="TeleStream ETL Pipeline")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--action", required=True, 
                       choices=["ingest", "transform", "aggregate", "optimize", "anomaly"])
    parser.add_argument("--batch-id", help="Batch ID for ingestion")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD)")
    parser.add_argument("--source-path", help="Source data path")
    
    args = parser.parse_args()
    
    pipeline = TelestreamETLPipeline(environment=args.env)
    
    if args.action == "ingest":
        pipeline.ingest_cdr_to_bronze(args.source_path, args.batch_id)
    
    elif args.action == "transform":
        pipeline.transform_bronze_to_silver(args.date)
    
    elif args.action == "aggregate":
        pipeline.create_gold_daily_usage_summary(args.date)
        pipeline.create_gold_network_performance(args.date)
    
    elif args.action == "optimize":
        for layer in ["bronze", "silver", "gold"]:
            path = f"{pipeline.data_lake_path}/{layer}/cdr"
            pipeline.optimize_delta_tables(path)
    
    elif args.action == "anomaly":
        anomalies = pipeline.detect_usage_anomalies()
        anomalies.show(50, truncate=False)