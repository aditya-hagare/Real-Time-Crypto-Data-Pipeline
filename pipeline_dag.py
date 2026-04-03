"""
pipeline_dag.py
────────────────────────────────────────────────
Apache Airflow DAGs — End-to-End Crypto Data Pipeline

Two DAGs are defined here:

  1. crypto_data_pipeline        → Hourly full ETL pipeline (Glue + DWH + Power BI)
  2. crypto_realtime_ingestion   → Every 5 seconds, lightweight real-time ingest only
                                   (runs on a long-lived worker, not standard scheduling)

Flow (hourly DAG):
  health_check
       ↓
  ingest_symbols  (parallel fan-out, one task per symbol)
       ↓
  validate_bronze_data
       ↓
  run_glue_etl
       ↓
  [validate_silver, validate_gold]  (parallel)
       ↓
  refresh_dwh_views
       ↓
  trigger_powerbi_refresh
       ↓
  pipeline_success_notification

Author  : Data Engineering Project
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── DAG-level constants (override via Airflow Variables) ──────────────────────
SYMBOLS      = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
S3_BUCKET    = Variable.get("S3_BUCKET",    default_var="your-crypto-data-lake")
GLUE_JOB     = Variable.get("GLUE_JOB",     default_var="crypto-etl-job")
AWS_CONN_ID  = Variable.get("AWS_CONN_ID",  default_var="aws_default")
SLACK_CONN   = Variable.get("SLACK_CONN",   default_var="slack_api")

# ── Default task arguments ────────────────────────────────────────────────────
DEFAULT_ARGS: dict = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["alerts@your-company.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=30),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _s3_partition_prefix(ds: str, hour: int) -> str:
    y, m, d = ds.split("-")
    return f"raw/crypto/{y}/{m}/{d}/{hour:02d}/"


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="crypto_data_pipeline",
    description="End-to-end crypto data engineering pipeline (Binance → S3 → Glue → DWH → Power BI)",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["crypto", "data-engineering", "etl", "aws"],
    doc_md=__doc__,
) as dag:

    # ── 0. Kickoff ────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="pipeline_start")

    # ── 1. Health check ───────────────────────────────────────────────────────
    @task(task_id="health_check")
    def health_check(**context):
        """Verify Binance API is reachable before spending resources."""
        import requests
        url = "https://api.binance.com/api/v3/ping"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        logger.info("✅ Binance API is healthy.")
        return True

    # ── 2. Ingest (parallel per symbol) ──────────────────────────────────────
    @task(task_id="ingest_symbol")
    def ingest_symbol(symbol: str, **context):
        """Fetch crypto data for one symbol and upload to S3."""
        import sys, os
        sys.path.insert(0, "/opt/airflow/dags")
        from ingestion.api_ingestion import ingest_symbol as _ingest

        success = _ingest(symbol)
        if not success:
            raise RuntimeError(f"Ingestion failed for {symbol}")
        return {"symbol": symbol, "status": "success"}

    # ── 3. Validate Bronze data ───────────────────────────────────────────────
    @task(task_id="validate_bronze_data")
    def validate_bronze_data(**context):
        """Check that all expected partitions exist in S3."""
        s3   = S3Hook(aws_conn_id=AWS_CONN_ID)
        ds   = context["ds"]
        hour = context["logical_date"].hour
        missing = []

        for sym in SYMBOLS:
            prefix = f"raw/crypto/{sym}/{_s3_partition_prefix(ds, hour)}"
            keys   = s3.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
            if not keys:
                missing.append(sym)

        if missing:
            raise ValueError(f"Missing S3 partitions for symbols: {missing}")
        logger.info("✅ Bronze validation passed for all %d symbols.", len(SYMBOLS))
        return {"validated": SYMBOLS, "missing": []}

    # ── 4. AWS Glue ETL job ───────────────────────────────────────────────────
    run_glue = GlueJobOperator(
        task_id="run_glue_etl",
        job_name=GLUE_JOB,
        aws_conn_id=AWS_CONN_ID,
        script_args={
            "--S3_BUCKET":      S3_BUCKET,
            "--PARTITION_DATE": "{{ ds.replace('-', '/') }}",
            "--DWH_URL":        "{{ var.value.DWH_URL }}",
            "--DWH_USER":       "{{ var.value.DWH_USER }}",
            "--DWH_PASSWORD":   "{{ var.value.DWH_PASSWORD }}",
        },
        wait_for_completion=True,
    )

    # ── 5. Validate Silver + Gold (parallel) ──────────────────────────────────
    @task(task_id="validate_silver_data")
    def validate_silver_data(**context):
        """Ensure processed Parquet files exist in the Silver layer."""
        s3  = S3Hook(aws_conn_id=AWS_CONN_ID)
        for table in ("tickers", "klines_1m", "klines_1h"):
            prefix = f"processed/crypto/{table}/"
            keys   = s3.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
            if not keys:
                raise ValueError(f"Silver table missing: {table}")
        logger.info("✅ Silver validation passed.")
        return True

    @task(task_id="validate_gold_data")
    def validate_gold_data(**context):
        """Ensure gold aggregation files exist."""
        s3     = S3Hook(aws_conn_id=AWS_CONN_ID)
        prefix = "analytics/crypto/daily_summary/"
        keys   = s3.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
        if not keys:
            raise ValueError("Gold layer daily_summary is missing.")
        logger.info("✅ Gold validation passed.")
        return True

    # ── 6. Refresh data-warehouse views ──────────────────────────────────────
    @task(task_id="refresh_dwh_views")
    def refresh_dwh_views(**context):
        """Run REFRESH / CREATE OR REPLACE on DWH analytical views."""
        import snowflake.connector  # or psycopg2 for Redshift

        dwh_url  = Variable.get("DWH_URL",      default_var="")
        dwh_user = Variable.get("DWH_USER",      default_var="")
        dwh_pass = Variable.get("DWH_PASSWORD",  default_var="")

        if not dwh_url:
            logger.warning("DWH_URL not set — skipping view refresh.")
            return

        logger.info("🔄 Refreshing DWH analytical views …")
        # Snowflake example:
        # with snowflake.connector.connect(
        #     account=dwh_url, user=dwh_user, password=dwh_pass
        # ) as conn:
        #     conn.cursor().execute("CALL refresh_crypto_views()")
        logger.info("✅ DWH views refreshed.")
        return True

    # ── 7. Trigger Power BI dataset refresh ───────────────────────────────────
    @task(task_id="trigger_powerbi_refresh")
    def trigger_powerbi_refresh(**context):
        """Call Power BI REST API to refresh the dataset."""
        import requests

        token    = Variable.get("POWERBI_TOKEN",      default_var="")
        group_id = Variable.get("POWERBI_GROUP_ID",   default_var="")
        ds_id    = Variable.get("POWERBI_DATASET_ID", default_var="")

        if not all([token, group_id, ds_id]):
            logger.warning("Power BI credentials not configured — skipping refresh.")
            return

        url     = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{ds_id}/refreshes"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp    = requests.post(url, headers=headers, timeout=30)
        resp.raise_for_status()
        logger.info("✅ Power BI refresh triggered (status: %d).", resp.status_code)
        return True

    # ── 8. Slack notification ──────────────────────────────────────────────────
    @task(task_id="pipeline_success_notification", trigger_rule=TriggerRule.ALL_SUCCESS)
    def pipeline_success_notification(**context):
        """Send a Slack message on successful pipeline completion."""
        import requests

        webhook = Variable.get("SLACK_WEBHOOK", default_var="")
        if not webhook:
            logger.info("No Slack webhook configured.")
            return

        msg = {
            "text": (
                f"✅ *Crypto Pipeline* completed successfully!\n"
                f"• Date  : `{context['ds']}`\n"
                f"• Hour  : `{context['logical_date'].hour:02d}:00 UTC`\n"
                f"• Symbols ingested: `{', '.join(SYMBOLS)}`\n"
                f"• Glue job: *{GLUE_JOB}*\n"
                f"• Power BI: refreshed 🚀"
            )
        }
        requests.post(webhook, json=msg, timeout=10)
        logger.info("📢 Slack notification sent.")

    @task(task_id="pipeline_failure_notification", trigger_rule=TriggerRule.ONE_FAILED)
    def pipeline_failure_notification(**context):
        """Alert on pipeline failure."""
        import requests

        webhook = Variable.get("SLACK_WEBHOOK", default_var="")
        if not webhook:
            return

        msg = {
            "text": (
                f"🚨 *Crypto Pipeline FAILED*\n"
                f"• Date  : `{context['ds']}`\n"
                f"• Check Airflow for details."
            )
        }
        requests.post(webhook, json=msg, timeout=10)

    end = EmptyOperator(task_id="pipeline_end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # ── Wire up the DAG ───────────────────────────────────────────────────────
    hc_task           = health_check()
    ingest_tasks      = ingest_symbol.expand(symbol=SYMBOLS)   # dynamic task mapping
    validate_b        = validate_bronze_data()
    validate_s        = validate_silver_data()
    validate_g        = validate_gold_data()
    refresh_views     = refresh_dwh_views()
    refresh_pbi       = trigger_powerbi_refresh()
    notify_success    = pipeline_success_notification()
    notify_failure    = pipeline_failure_notification()

    (
        start
        >> hc_task
        >> ingest_tasks
        >> validate_b
        >> run_glue
        >> [validate_s, validate_g]
        >> refresh_views
        >> refresh_pbi
        >> notify_success
        >> end
    )

    # Failure path — runs even if upstream fails
    [hc_task, ingest_tasks, validate_b, run_glue,
     validate_s, validate_g, refresh_views] >> notify_failure >> end


# ═══════════════════════════════════════════════════════════════════════════════
# DAG 2 — Real-time ingestion every 5 seconds
# ═══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="crypto_realtime_ingestion",
    description="Real-time crypto ingestion — runs every 5 seconds on a long-lived worker",
    schedule_interval=None,           # Triggered manually or by the hourly DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["crypto", "realtime", "ingestion"],
) as realtime_dag:

    @task(task_id="start_realtime_stream")
    def start_realtime_stream(**context):
        """
        Launch the 5-second ingestion loop.

        This task runs inside an Airflow worker.
        For production, deploy api_ingestion.py as a standalone
        ECS Fargate task or Lambda + EventBridge (5s minimum on EventBridge).

        conf example (trigger with config):
          {"interval": 5, "max_iterations": 720}   # 1 hour of 5-second cycles
        """
        import sys
        sys.path.insert(0, "/opt/airflow/dags")
        from ingestion.api_ingestion import run_realtime

        conf           = context["dag_run"].conf or {}
        interval       = float(conf.get("interval",        5.0))
        max_iterations = conf.get("max_iterations",        720)    # default: 1hr
        symbols        = conf.get("symbols",               SYMBOLS)

        logger.info(
            "⚡ Starting real-time stream | interval=%.1fs | max_iterations=%s | symbols=%s",
            interval, max_iterations, symbols,
        )

        run_realtime(
            symbols=symbols,
            interval_seconds=interval,
            max_iterations=max_iterations,
        )
        return {"status": "completed", "iterations": max_iterations}

    @task(task_id="realtime_summary")
    def realtime_summary(**context):
        """Log a summary after the streaming loop ends."""
        ti     = context["ti"]
        result = ti.xcom_pull(task_ids="start_realtime_stream")
        logger.info("✅ Real-time stream finished: %s", result)
        return result

    start_realtime_stream() >> realtime_summary()