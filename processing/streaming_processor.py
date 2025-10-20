"""
Real-Time Stream Processing with Kafka + Spark Structured Streaming
Handles network events with stateful aggregations and windowing
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import DataStreamWriter
from delta import configure_spark_with_delta_pip
import logging
from typing import Dict, List
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealtimeNetworkMonitoring:
    """Real-time network event processing and anomaly detection"""
    
    def __init__(self, kafka_brokers: str = "kafka:9092"):
        self.kafka_brokers = kafka_brokers
        self.spark = self._create_spark_session()
        self.checkpoint_location = "/tmp/checkpoints"
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for streaming"""
        builder = (
            SparkSession.builder
            .appName("TeleStream-RealtimeMonitoring")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
            .config("spark.sql.streaming.stateStore.providerClass", 
                   "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.shuffle.partitions", "10")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    # ========================================
    # KAFKA STREAM INGESTION
    # ========================================
    
    def read_network_events_stream(self) -> DataFrame:
        """Read real-time network events from Kafka"""
        
        # Define schema for network events
        event_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("cell_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("active_users", IntegerType(), True),
            StructField("bandwidth_utilization_percent", DoubleType(), True),
            StructField("error_rate_percent", DoubleType(), True),
            StructField("avg_throughput_mbps", DoubleType(), True),
            StructField("signal_strength_dbm", IntegerType(), True),
            StructField("latency_ms", IntegerType(), True),
            StructField("temperature_celsius", DoubleType(), True)
        ])
        
        # Read from Kafka
        df_raw = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", "network-events") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON payload
        df_parsed = df_raw \
            .select(
                F.col("key").cast("string").alias("message_key"),
                F.from_json(F.col("value").cast("string"), event_schema).alias("data"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("partition"),
                F.col("offset")
            ) \
            .select("message_key", "data.*", "kafka_timestamp", "partition", "offset")
        
        # Add processing metadata
        df_enriched = df_parsed \
            .withColumn("processing_time", F.current_timestamp()) \
            .withColumn("ingestion_delay_seconds", 
                       F.unix_timestamp("processing_time") - F.unix_timestamp("event_timestamp")) \
            .withWatermark("event_timestamp", "10 minutes")
        
        return df_enriched
    
    # ========================================
    # REAL-TIME AGGREGATIONS
    # ========================================
    
    def compute_cell_metrics_windowed(self, df_stream: DataFrame) -> DataFrame:
        """
        Compute real-time metrics per cell tower with tumbling windows
        5-minute windows, updated every minute
        """
        
        df_windowed = df_stream \
            .groupBy(
                F.window("event_timestamp", "5 minutes", "1 minute"),
                "cell_id"
            ) \
            .agg(
                F.count("event_id").alias("event_count"),
                F.avg("bandwidth_utilization_percent").alias("avg_bandwidth_util"),
                F.max("bandwidth_utilization_percent").alias("max_bandwidth_util"),
                F.avg("error_rate_percent").alias("avg_error_rate"),
                F.max("error_rate_percent").alias("max_error_rate"),
                F.avg("active_users").alias("avg_active_users"),
                F.max("active_users").alias("max_active_users"),
                F.avg("latency_ms").alias("avg_latency_ms"),
                F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
                F.sum(F.when(F.col("event_type") == "congestion", 1).otherwise(0)).alias("congestion_events"),
                F.sum(F.when(F.col("event_type") == "outage", 1).otherwise(0)).alias("outage_events"),
                F.sum(F.when(F.col("error_rate_percent") > 5, 1).otherwise(0)).alias("high_error_events")
            ) \
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "cell_id",
                "*"
            ) \
            .drop("window")
        
        # Add health score (0-100)
        df_scored = df_windowed \
            .withColumn("health_score",
                F.greatest(
                    F.lit(0),
                    F.lit(100) - 
                    (F.col("avg_error_rate") * 10) -
                    (F.when(F.col("avg_bandwidth_util") > 80, 20).otherwise(0)) -
                    (F.col("congestion_events") * 5) -
                    (F.col("outage_events") * 30)
                ))
        
        return df_scored
    
    def detect_realtime_anomalies(self, df_stream: DataFrame) -> DataFrame:
        """
        Real-time anomaly detection using sliding window statistics
        """
        
        # Calculate rolling statistics (30-minute window)
        window_spec_30m = F.window("event_timestamp", "30 minutes")
        
        df_stats = df_stream \
            .groupBy(window_spec_30m, "cell_id") \
            .agg(
                F.avg("bandwidth_utilization_percent").alias("avg_bw"),
                F.stddev("bandwidth_utilization_percent").alias("stddev_bw"),
                F.avg("error_rate_percent").alias("avg_error"),
                F.stddev("error_rate_percent").alias("stddev_error")
            )
        
        # Join current events with statistics
        df_with_stats = df_stream.alias("current") \
            .join(
                df_stats.alias("stats"),
                (F.col("current.cell_id") == F.col("stats.cell_id")) &
                (F.col("current.event_timestamp") >= F.col("stats.window.start")) &
                (F.col("current.event_timestamp") <= F.col("stats.window.end")),
                "left"
            )
        
        # Detect anomalies
        df_anomalies = df_with_stats \
            .withColumn("bw_zscore",
                       (F.col("bandwidth_utilization_percent") - F.col("avg_bw")) / 
                       F.coalesce(F.col("stddev_bw"), F.lit(1))) \
            .withColumn("error_zscore",
                       (F.col("error_rate_percent") - F.col("avg_error")) / 
                       F.coalesce(F.col("stddev_error"), F.lit(1))) \
            .withColumn("is_anomaly",
                       (F.abs(F.col("bw_zscore")) > 3) | 
                       (F.abs(F.col("error_zscore")) > 3) |
                       (F.col("bandwidth_utilization_percent") > 95) |
                       (F.col("error_rate_percent") > 10)) \
            .withColumn("anomaly_severity",
                       F.when(F.col("bandwidth_utilization_percent") > 98, "CRITICAL")
                       .when(F.col("error_rate_percent") > 15, "CRITICAL")
                       .when(F.abs(F.col("bw_zscore")) > 4, "HIGH")
                       .when(F.abs(F.col("error_zscore")) > 4, "HIGH")
                       .otherwise("MEDIUM")) \
            .filter(F.col("is_anomaly"))
        
        return df_anomalies
    
    # ========================================
    # STATEFUL PROCESSING
    # ========================================
    
    def track_cell_state(self, df_stream: DataFrame) -> DataFrame:
        """
        Maintain stateful tracking of cell tower status
        Uses arbitrary stateful processing
        """
        
        def update_cell_state(cell_id, events, state):
            """Update function for stateful processing"""
            if state.exists:
                old_state = state.get
                total_events = old_state['total_events'] + len(events)
                cumulative_errors = old_state['cumulative_errors'] + sum(e['error_rate_percent'] for e in events)
            else:
                total_events = len(events)
                cumulative_errors = sum(e['error_rate_percent'] for e in events)
            
            new_state = {
                'cell_id': cell_id,
                'total_events': total_events,
                'cumulative_errors': cumulative_errors,
                'avg_error_rate': cumulative_errors / total_events,
                'last_updated': events[-1]['event_timestamp']
            }
            
            state.update(new_state)
            return new_state
        
        # Apply stateful processing
        df_stateful = df_stream \
            .groupByKey(lambda row: row.cell_id) \
            .mapGroupsWithState(
                update_cell_state,
                outputMode="update",
                timeoutConf="ProcessingTimeTimeout"
            )
        
        return df_stateful
    
    # ========================================
    # SINK OPERATIONS
    # ========================================
    
    def write_to_delta_lake(self, df_stream: DataFrame, table_name: str) -> DataStreamWriter:
        """Write stream to Delta Lake with exactly-once semantics"""
        
        output_path = f"/tmp/datalake/streaming/{table_name}"
        checkpoint_path = f"{self.checkpoint_location}/{table_name}"
        
        query = df_stream \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", output_path) \
            .partitionBy("event_date") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def write_to_kafka(self, df_stream: DataFrame, topic: str) -> DataStreamWriter:
        """Write processed stream back to Kafka"""
        
        # Convert to JSON
        df_json = df_stream \
            .select(
                F.col("cell_id").alias("key"),
                F.to_json(F.struct("*")).alias("value")
            )
        
        query = df_json \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("topic", topic) \
            .option("checkpointLocation", f"{self.checkpoint_location}/kafka_{topic}") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        return query
    
    def write_to_console(self, df_stream: DataFrame) -> DataStreamWriter:
        """Write to console for debugging"""
        
        query = df_stream \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 50) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        return query
    
    def write_to_postgres(self, df_stream: DataFrame, table_name: str) -> DataStreamWriter:
        """Write stream to PostgreSQL using foreachBatch"""
        
        def write_batch_to_postgres(batch_df: DataFrame, batch_id: int):
            """Custom sink function for PostgreSQL"""
            if batch_df.count() > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://postgres:5432/telestream_dw") \
                    .option("dbtable", table_name) \
                    .option("user", "telestream") \
                    .option("password", "telestream123") \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Batch {batch_id}: Written {batch_df.count()} records to {table_name}")
        
        query = df_stream \
            .writeStream \
            .foreachBatch(write_batch_to_postgres) \
            .option("checkpointLocation", f"{self.checkpoint_location}/postgres_{table_name}") \
            .trigger(processingTime="1 minute") \
            .start()
        
        return query
    
    # ========================================
    # ALERT GENERATION
    # ========================================
    
    def generate_alerts(self, df_anomalies: DataFrame) -> DataStreamWriter:
        """Generate alerts for critical anomalies"""
        
        def send_alert(batch_df: DataFrame, batch_id: int):
            """Send alerts via custom notification service"""
            critical_alerts = batch_df.filter(F.col("anomaly_severity") == "CRITICAL")
            
            for row in critical_alerts.collect():
                alert_payload = {
                    "alert_id": f"ALERT-{batch_id}-{row.event_id}",
                    "severity": row.anomaly_severity,
                    "cell_id": row.cell_id,
                    "event_type": row.event_type,
                    "metrics": {
                        "bandwidth_util": row.bandwidth_utilization_percent,
                        "error_rate": row.error_rate_percent,
                        "active_users": row.active_users
                    },
                    "timestamp": row.event_timestamp.isoformat()
                }
                
                logger.critical(f"ALERT: {json.dumps(alert_payload)}")
                # In production: send to PagerDuty, Slack, email, etc.
        
        query = df_anomalies \
            .writeStream \
            .foreachBatch(send_alert) \
            .option("checkpointLocation", f"{self.checkpoint_location}/alerts") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    # ========================================
    # MAIN PIPELINE ORCHESTRATION
    # ========================================
    
    def run_realtime_monitoring_pipeline(self):
        """Main streaming pipeline"""
        logger.info("Starting real-time network monitoring pipeline...")
        
        # 1. Read stream
        df_events = self.read_network_events_stream()
        
        # 2. Compute windowed metrics
        df_metrics = self.compute_cell_metrics_windowed(df_events)
        
        # 3. Detect anomalies
        df_anomalies = self.detect_realtime_anomalies(df_events)
        
        # 4. Write metrics to Delta Lake
        query_metrics = self.write_to_delta_lake(
            df_metrics.withColumn("event_date", F.to_date("window_start")),
            "cell_metrics_realtime"
        )
        
        # 5. Write anomalies to PostgreSQL
        query_anomalies_db = self.write_to_postgres(
            df_anomalies,
            "fact_network_anomalies"
        )
        
        # 6. Publish anomalies to Kafka for downstream consumers
        query_anomalies_kafka = self.write_to_kafka(
            df_anomalies.select("cell_id", "event_type", "anomaly_severity", "event_timestamp"),
            "network-anomalies"
        )
        
        # 7. Generate critical alerts
        query_alerts = self.generate_alerts(df_anomalies)
        
        # Wait for termination
        self.spark.streams.awaitAnyTermination()


# ========================================
# KAFKA PRODUCER (Data Simulator)
# ========================================

class NetworkEventProducer:
    """Simulate network events and produce to Kafka"""
    
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = "network-events"
    
    def generate_event(self, cell_id: str) -> Dict:
        """Generate synthetic network event"""
        import random
        from datetime import datetime
        
        event_types = ["normal", "congestion", "handover", "interference", "outage"]
        weights = [0.7, 0.15, 0.08, 0.05, 0.02]
        
        event_type = random.choices(event_types, weights=weights)[0]
        
        # Simulate realistic metrics based on event type
        if event_type == "congestion":
            bandwidth_util = random.uniform(85, 99)
            error_rate = random.uniform(3, 15)
            active_users = random.randint(800, 1200)
        elif event_type == "outage":
            bandwidth_util = 0
            error_rate = 100
            active_users = 0
        else:
            bandwidth_util = random.uniform(30, 75)
            error_rate = random.uniform(0.1, 2)
            active_users = random.randint(100, 600)
        
        event = {
            "event_id": f"EVT-{datetime.now().timestamp()}-{random.randint(1000, 9999)}",
            "cell_id": cell_id,
            "event_type": event_type,
            "event_timestamp": datetime.now().isoformat(),
            "active_users": active_users,
            "bandwidth_utilization_percent": round(bandwidth_util, 2),
            "error_rate_percent": round(error_rate, 2),
            "avg_throughput_mbps": round(random.uniform(10, 100), 2),
            "signal_strength_dbm": random.randint(-100, -50),
            "latency_ms": random.randint(10, 200),
            "temperature_celsius": round(random.uniform(20, 70), 1)
        }
        
        return event
    
    def produce_events(self, num_cells: int = 100, duration_minutes: int = 60):
        """Continuously produce events"""
        import time
        
        cell_ids = [f"CELL-{str(i).zfill(4)}" for i in range(1, num_cells + 1)]
        
        logger.info(f"Starting event production for {duration_minutes} minutes...")
        start_time = time.time()
        
        while (time.time() - start_time) < duration_minutes * 60:
            # Produce events for random cells
            for _ in range(random.randint(5, 20)):
                cell_id = random.choice(cell_ids)
                event = self.generate_event(cell_id)
                
                self.producer.send(self.topic, value=event, key=cell_id.encode())
            
            time.sleep(1)  # 1 second between batches
        
        self.producer.flush()
        logger.info("Event production completed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time Network Monitoring")
    parser.add_argument("--mode", choices=["produce", "consume"], required=True)
    parser.add_argument("--kafka-brokers", default="kafka:9092")
    parser.add_argument("--num-cells", type=int, default=100)
    parser.add_argument("--duration", type=int, default=60)
    
    args = parser.parse_args()
    
    if args.mode == "produce":
        producer = NetworkEventProducer(args.kafka_brokers)
        producer.produce_events(args.num_cells, args.duration)
    
    elif args.mode == "consume":
        monitor = RealtimeNetworkMonitoring(args.kafka_brokers)
        monitor.run_realtime_monitoring_pipeline()