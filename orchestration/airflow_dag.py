"""
Production-grade Airflow DAG for TeleStream ETL Pipeline
Features: Dynamic task generation, data quality checks, SLA monitoring, alerting
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)


# ========================================
# DAG DEFAULT ARGUMENTS
# ========================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-team@telestream.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=2),
    "execution_timeout": timedelta(hours=4),
    "on_failure_callback": lambda context: send_slack_alert(context, "FAILURE"),
    "on_success_callback": None,
    "on_retry_callback": lambda context: send_slack_alert(context, "RETRY"),
}


# ========================================
# HELPER FUNCTIONS
# ========================================


def send_slack_alert(context: dict, status: str):
    """Send alert to Slack webhook"""
    import requests

    webhook_url = Variable.get("slack_webhook_url", default_var=None)
    if not webhook_url:
        logger.warning("Slack webhook not configured")
        return

    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    message = {
        "text": f"*{status}*: DAG `{dag_id}` | Task `{task_instance.task_id}`",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Status*: {status}\n*DAG*: {dag_id}\n*Task*: {task_instance.task_id}\n*Execution Date*: {execution_date}",
                },
            }
        ],
    }

    try:
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")


def check_data_freshness(**context):
    """Check if source data is available and fresh"""
    from datetime import datetime, timedelta
    import boto3

    execution_date = context["execution_date"]
    expected_file = (
        f"s3://telestream-raw/cdr/{execution_date.strftime('%Y-%m-%d')}/cdr_data.csv"
    )

    # Check if file exists and was created within last 2 hours
    s3 = boto3.client("s3")
    try:
        response = s3.head_object(
            Bucket="telestream-raw",
            Key=f"cdr/{execution_date.strftime('%Y-%m-%d')}/cdr_data.csv",
        )
        last_modified = response["LastModified"]

        age_hours = (
            datetime.now(last_modified.tzinfo) - last_modified
        ).total_seconds() / 3600

        if age_hours > 2:
            raise AirflowException(f"Data is too old: {age_hours:.2f} hours")

        logger.info(f"Data freshness check passed: {age_hours:.2f} hours old")
        return True
    except Exception as e:
        raise AirflowException(f"Data freshness check failed: {e}")


def validate_schema(**context):
    """Validate incoming data schema"""
    import pandas as pd
    from great_expectations.core import ExpectationConfiguration
    from great_expectations.dataset import PandasDataset

    execution_date = context["execution_date"]
    file_path = f"/data/raw/cdr_{execution_date.strftime('%Y%m%d')}.csv"

    # Read sample data
    df = pd.read_csv(file_path, nrows=1000)
    ge_df = PandasDataset(df)

    # Define expectations
    expectations = [
        ge_df.expect_table_columns_to_match_ordered_list(
            [
                "call_id",
                "customer_id",
                "call_type",
                "call_start_time",
                "duration_seconds",
                "data_volume_mb",
                "call_cost",
            ]
        ),
        ge_df.expect_column_values_to_not_be_null("call_id"),
        ge_df.expect_column_values_to_not_be_null("customer_id"),
        ge_df.expect_column_values_to_be_in_set("call_type", ["voice", "sms", "data"]),
        ge_df.expect_column_values_to_be_between("duration_seconds", 0, 86400),
        ge_df.expect_column_values_to_be_between("data_volume_mb", 0, 10000),
    ]

    # Check all expectations
    failed = []
    for expectation in expectations:
        if not expectation.success:
            failed.append(expectation.expectation_config.to_json_dict())

    if failed:
        raise AirflowException(
            f"Schema validation failed: {json.dumps(failed, indent=2)}"
        )

    logger.info("Schema validation passed")


def run_spark_etl_job(job_type: str, **context):
    """Execute Spark ETL job"""
    import subprocess

    execution_date = context["execution_date"]
    batch_id = f"BATCH-{execution_date.strftime('%Y%m%d-%H%M%S')}"

    cmd = [
        "spark-submit",
        "--master",
        "local[*]",
        "--driver-memory",
        "4g",
        "--executor-memory",
        "4g",
        "--conf",
        "spark.sql.adaptive.enabled=true",
        "--packages",
        "io.delta:delta-core_2.12:2.4.0",
        "/opt/airflow/dags/processing/batch_transform.py",
        "--env",
        "prod",
        "--action",
        job_type,
        "--batch-id",
        batch_id,
        "--date",
        execution_date.strftime("%Y-%m-%d"),
    ]

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Spark job completed: {result.stdout}")
        context["task_instance"].xcom_push(key="batch_id", value=batch_id)
        return batch_id
    except subprocess.CalledProcessError as e:
        logger.error(f"Spark job failed: {e.stderr}")
        raise AirflowException(f"Spark ETL failed: {e.stderr}")


def run_data_quality_checks(**context):
    """Comprehensive data quality validation"""
    from great_expectations.data_context import DataContext

    execution_date = context["execution_date"]
    batch_id = context["task_instance"].xcom_pull(
        task_ids="transform_bronze_to_silver", key="batch_id"
    )

    # Initialize Great Expectations context
    ge_context = DataContext("/opt/airflow/great_expectations")

    # Run checkpoint
    checkpoint_name = "silver_data_quality_checkpoint"
    results = ge_context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        batch_request={
            "datasource_name": "delta_lake_datasource",
            "data_asset_name": "silver_cdr",
            "batch_identifiers": {"batch_id": batch_id},
        },
    )

    if not results.success:
        failed_validations = results.list_validation_results()
        raise AirflowException(f"Data quality checks failed: {failed_validations}")

    # Log metrics to database
    hook = PostgresHook(postgres_conn_id="telestream_warehouse")

    total_checks = len(results.list_validation_results())
    passed_checks = sum(1 for r in results.list_validation_results() if r.success)

    hook.run(
        """
        INSERT INTO data_quality_log (
            table_name, check_name, check_timestamp, 
            records_checked, records_passed, pass_rate, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """,
        parameters=(
            "silver_cdr",
            checkpoint_name,
            datetime.now(),
            total_checks,
            passed_checks,
            (passed_checks / total_checks) * 100,
            "PASSED",
        ),
    )

    logger.info(f"Data quality checks passed: {passed_checks}/{total_checks}")


def check_if_reprocess_needed(**context):
    """Decide if data needs reprocessing based on quality scores"""
    hook = PostgresHook(postgres_conn_id="telestream_warehouse")

    execution_date = context["execution_date"]

    # Query recent quality scores
    result = hook.get_first(
        """
        SELECT pass_rate FROM data_quality_log
        WHERE table_name = 'silver_cdr'
        AND check_timestamp >= %s
        ORDER BY check_timestamp DESC LIMIT 1
    """,
        parameters=(execution_date,),
    )

    if result and result[0] < 95:
        logger.warning(f"Data quality below threshold: {result[0]}%")
        return "reprocess_data"
    else:
        logger.info(f"Data quality acceptable: {result[0]}%")
        return "proceed_to_gold"


def aggregate_to_gold_layer(**context):
    """Create gold layer aggregations"""
    execution_date = context["execution_date"]

    # Run aggregation job
    return run_spark_etl_job("aggregate", **context)


def update_materialized_views(**context):
    """Refresh materialized views in PostgreSQL"""
    hook = PostgresHook(postgres_conn_id="telestream_warehouse")

    views = ["mv_top_customers_usage", "mv_network_performance"]

    for view in views:
        logger.info(f"Refreshing materialized view: {view}")
        hook.run(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")

    logger.info("All materialized views refreshed")


def optimize_delta_tables(**context):
    """Run Delta Lake optimization"""
    import subprocess

    cmd = [
        "spark-submit",
        "--master",
        "local[*]",
        "/opt/airflow/dags/processing/batch_transform.py",
        "--env",
        "prod",
        "--action",
        "optimize",
    ]

    subprocess.run(cmd, check=True)
    logger.info("Delta table optimization completed")


def generate_lineage_metadata(**context):
    """Generate data lineage information"""
    from openlineage.client import OpenLineageClient
    from openlineage.client.event import RunEvent, RunState, Job, Run

    execution_date = context["execution_date"]
    batch_id = context["task_instance"].xcom_pull(
        task_ids="transform_bronze_to_silver", key="batch_id"
    )

    # Initialize OpenLineage client
    client = OpenLineageClient(url=Variable.get("openlineage_url"))

    # Create lineage event
    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now().isoformat(),
        job=Job(namespace="telestream", name="cdr_etl_pipeline"),
        run=Run(runId=batch_id),
        inputs=[],
        outputs=[],
    )

    client.emit(event)
    logger.info(f"Lineage metadata generated for batch {batch_id}")


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id="telestream_cdr_batch_pipeline",
    default_args=default_args,
    description="Production ETL pipeline for CDR data with quality checks",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["telestream", "batch", "production"],
    doc_md="""
    ## TeleStream CDR Batch Pipeline
    
    This DAG orchestrates the complete ETL process for Call Detail Records:
    
    1. **Data Validation**: Schema validation and freshness checks
    2. **Bronze Layer**: Raw data ingestion with deduplication
    3. **Silver Layer**: Cleaned and enriched data
    4. **Gold Layer**: Business-ready aggregations
    5. **Quality Checks**: Comprehensive data quality validation
    6. **Optimization**: Delta Lake optimization and vacuum
    
    **SLA**: 2 hours
    **Retry Policy**: 3 retries with exponential backoff
    """,
) as dag:

    # ========================================
    # TASK GROUP: PRE-PROCESSING VALIDATION
    # ========================================

    with TaskGroup(group_id="pre_validation") as pre_validation:

        check_freshness = PythonOperator(
            task_id="check_data_freshness",
            python_callable=check_data_freshness,
            provide_context=True,
        )

        validate_schema_task = PythonOperator(
            task_id="validate_schema",
            python_callable=validate_schema,
            provide_context=True,
        )

        check_freshness >> validate_schema_task

    # ========================================
    # TASK GROUP: BRONZE LAYER INGESTION
    # ========================================

    with TaskGroup(group_id="bronze_ingestion") as bronze_ingestion:

        ingest_cdr = PythonOperator(
            task_id="ingest_cdr_to_bronze",
            python_callable=lambda **context: run_spark_etl_job("ingest", **context),
            provide_context=True,
        )

        log_bronze_metrics = PostgresOperator(
            task_id="log_bronze_metrics",
            postgres_conn_id="telestream_warehouse",
            sql="""
                INSERT INTO data_lineage (
                    source_table, target_table, transformation_name,
                    execution_timestamp, status
                ) VALUES ('raw_cdr', 'bronze_cdr', 'bronze_ingestion', NOW(), 'SUCCESS');
            """,
        )

        ingest_cdr >> log_bronze_metrics

    # ========================================
    # TASK GROUP: SILVER LAYER TRANSFORMATION
    # ========================================

    with TaskGroup(group_id="silver_transformation") as silver_transformation:

        transform_silver = PythonOperator(
            task_id="transform_bronze_to_silver",
            python_callable=lambda **context: run_spark_etl_job("transform", **context),
            provide_context=True,
        )

        quality_checks = PythonOperator(
            task_id="run_data_quality_checks",
            python_callable=run_data_quality_checks,
            provide_context=True,
        )

        transform_silver >> quality_checks

    # ========================================
    # BRANCHING: REPROCESS OR CONTINUE
    # ========================================

    branch_quality = BranchPythonOperator(
        task_id="check_quality_threshold",
        python_callable=check_if_reprocess_needed,
        provide_context=True,
    )

    reprocess = BashOperator(
        task_id="reprocess_data",
        bash_command='echo "Reprocessing data due to quality issues" && exit 1',
    )

    # ========================================
    # TASK GROUP: GOLD LAYER AGGREGATION
    # ========================================

    with TaskGroup(group_id="gold_aggregation") as gold_aggregation:

        proceed = BashOperator(
            task_id="proceed_to_gold", bash_command='echo "Proceeding to gold layer"'
        )

        create_daily_summary = PythonOperator(
            task_id="create_daily_usage_summary",
            python_callable=aggregate_to_gold_layer,
            provide_context=True,
        )

        update_mvs = PythonOperator(
            task_id="update_materialized_views",
            python_callable=update_materialized_views,
            provide_context=True,
        )

        proceed >> create_daily_summary >> update_mvs

    # ========================================
    # POST-PROCESSING TASKS
    # ========================================

    optimize_tables = PythonOperator(
        task_id="optimize_delta_tables",
        python_callable=optimize_delta_tables,
        provide_context=True,
    )

    generate_lineage = PythonOperator(
        task_id="generate_lineage_metadata",
        python_callable=generate_lineage_metadata,
        provide_context=True,
    )

    send_success_notification = EmailOperator(
        task_id="send_success_email",
        to=["data-team@telestream.com"],
        subject="TeleStream ETL Pipeline - Success",
        html_content="""
        <h3>Pipeline Execution Successful</h3>
        <p>Execution Date: {{ ds }}</p>
        <p>Duration: {{ task_instance.duration }} seconds</p>
        """,
    )

    # ========================================
    # TASK DEPENDENCIES
    # ========================================

    pre_validation >> bronze_ingestion >> silver_transformation >> branch_quality
    branch_quality >> [reprocess, gold_aggregation]
    gold_aggregation >> optimize_tables >> generate_lineage >> send_success_notification
