"""
spark_job.py
────────────────────────────────────────────────
AWS Glue / PySpark ETL job.

Reads raw JSON from the S3 Bronze layer, cleans + enriches the data,
computes technical indicators, then writes Parquet to the Silver layer
and aggregated metrics to the Gold layer.

Layers:
  Bronze  s3://<BUCKET>/raw/market/
  Silver  s3://<BUCKET>/processed/market/
  Gold    s3://<BUCKET>/analytics/market/

Author  : Data Engineering Project
"""

import sys
import logging
from datetime import datetime, timezone

# ── Glue / Spark imports ──────────────────────────────────────────────────────
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    RUNNING_IN_GLUE = True
except ImportError:
    RUNNING_IN_GLUE = False

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)


# ── Job parameters — ALL OPTIONAL with sensible defaults ─────────────────────
def get_job_params() -> dict:
    """
    Read parameters from Glue job arguments.
    Every parameter has a default value — nothing is required.
    This means the job can run even if some parameters are not passed.
    """
    # Default values — used when parameter is not passed
    params = {
        "JOB_NAME":       "spark-etl-job",
        "S3_BUCKET":      "crypto-data-lake-aditya",
        "BRONZE_PREFIX":  "raw/market",
        "SILVER_PREFIX":  "processed/market",
        "GOLD_PREFIX":    "analytics/market",
        "DWH_URL":        "",
        "DWH_USER":       "",
        "DWH_PASSWORD":   "",
        "DWH_TABLE":      "crypto_analytics",
        "PARTITION_DATE": datetime.now(timezone.utc).strftime("%Y/%m/%d"),
    }

    if RUNNING_IN_GLUE:
        # Find which args were actually passed in
        passed_keys = []
        for arg in sys.argv:
            if arg.startswith("--"):
                key = arg[2:]
                if key in params:
                    passed_keys.append(key)

        # Only resolve args that were actually passed
        if passed_keys:
            try:
                resolved = getResolvedOptions(sys.argv, passed_keys)
                params.update(resolved)
            except Exception as e:
                logger.warning("Could not resolve some args: %s", e)

    # Clean "none" values → treat as empty string
    for key in ["DWH_URL", "DWH_USER", "DWH_PASSWORD"]:
        val = params.get(key, "")
        if str(val).lower() in ("none", "null", "n/a", ""):
            params[key] = ""

    logger.info("📋 Job params loaded:")
    for k, v in params.items():
        display = "***" if "PASSWORD" in k and v else v
        logger.info("   %-20s = %s", k, display)

    return params


# ── Spark session ─────────────────────────────────────────────────────────────
def build_spark(job_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(job_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if not RUNNING_IN_GLUE:
        builder = builder.master("local[*]")
    return builder.getOrCreate()


# ── Read Bronze layer ─────────────────────────────────────────────────────────
def read_bronze(spark: SparkSession, bucket: str, prefix: str, date: str) -> DataFrame:
    path = f"s3://{bucket}/{prefix}/"
    logger.info("📂 Reading bronze from: %s (recursive)", path)
    try:
        df = (
            spark.read
            .option("multiline", "true")
            .option("recursiveFileLookup", "true")
            .option("inferSchema", "true")
            .option("samplingRatio", "1.0")
            .json(path)
        )
        logger.info("✅ Bronze records loaded: %d", df.count())
        return df
    except Exception as e:
        logger.error("❌ Failed to read bronze: %s", e)
        # Try reading as raw text and parse manually
        logger.info("🔄 Trying raw text read ...")
        rdd = spark.sparkContext.wholeTextFiles(path + "**/*.json")
        df = spark.read.option("multiline", "true").json(rdd.values())
        logger.info("✅ Bronze loaded via RDD: %d", df.count())
        return df


# ── Extract & clean ticker data ───────────────────────────────────────────────
def extract_ticker(df: DataFrame) -> DataFrame:
    logger.info("🔄 Extracting ticker data ...")
    return (
        df.select(
            F.col("metadata.symbol").alias("symbol"),
            F.col("metadata.name").alias("name"),
            F.col("metadata.asset_class").alias("asset_class"),
            F.col("metadata.ingested_at").alias("ingested_at"),
            F.col("ticker.lastPrice").cast(DoubleType()).alias("last_price"),
            F.col("ticker.openPrice").cast(DoubleType()).alias("open_price"),
            F.col("ticker.highPrice").cast(DoubleType()).alias("high_price"),
            F.col("ticker.lowPrice").cast(DoubleType()).alias("low_price"),
            F.col("ticker.volume").cast(DoubleType()).alias("volume"),
            F.col("ticker.quoteVolume").cast(DoubleType()).alias("quote_volume"),
            F.col("ticker.priceChange").cast(DoubleType()).alias("price_change"),
            F.col("ticker.priceChangePercent").cast(DoubleType()).alias("price_change_pct"),
            F.col("ticker.weightedAvgPrice").cast(DoubleType()).alias("vwap"),
            F.col("ticker.count").alias("trade_count"),
        )
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("date", F.to_date("ingested_at"))
        .withColumn("hour", F.hour("ingested_at"))
        .dropna(subset=["symbol", "last_price"])
        .dropDuplicates(["symbol", "ingested_at"])
        .filter(F.col("last_price") > 0)
    )


# ── Extract & clean klines ────────────────────────────────────────────────────
def extract_klines(df: DataFrame, interval: str = "klines_1m") -> DataFrame:
    logger.info("🔄 Extracting klines: %s ...", interval)
    return (
        df.select(
            F.col("metadata.symbol").alias("symbol"),
            F.col("metadata.ingested_at").alias("ingested_at"),
            F.explode(F.col(interval)).alias("kline"),
        )
        .select(
            "symbol", "ingested_at",
            F.col("kline.open_time").alias("open_time_ms"),
            F.col("kline.open").cast(DoubleType()).alias("open"),
            F.col("kline.high").cast(DoubleType()).alias("high"),
            F.col("kline.low").cast(DoubleType()).alias("low"),
            F.col("kline.close").cast(DoubleType()).alias("close"),
            F.col("kline.volume").cast(DoubleType()).alias("volume"),
            F.col("kline.num_trades").alias("num_trades"),
        )
        .withColumn("open_time", F.to_timestamp(F.col("open_time_ms") / 1000))
        .withColumn("interval", F.lit(interval.replace("klines_", "")))
        .drop("open_time_ms")
        .dropna(subset=["symbol", "close"])
        .dropDuplicates(["symbol", "interval", "open_time"])
        .filter(F.col("close") > 0)
    )


# ── Technical indicators ──────────────────────────────────────────────────────
def add_technical_indicators(df: DataFrame) -> DataFrame:
    logger.info("📊 Computing technical indicators ...")
    sym_time = Window.partitionBy("symbol", "interval").orderBy("open_time")

    # Simple Moving Averages
    for n in [7, 14, 21]:
        w = sym_time.rowsBetween(-n + 1, 0)
        df = df.withColumn(f"sma_{n}", F.avg("close").over(w))

    # Bollinger Bands (20-period)
    bb_w = sym_time.rowsBetween(-19, 0)
    df = (
        df.withColumn("bb_mid",   F.avg("close").over(bb_w))
          .withColumn("bb_std",   F.stddev("close").over(bb_w))
          .withColumn("bb_upper", F.col("bb_mid") + 2 * F.col("bb_std"))
          .withColumn("bb_lower", F.col("bb_mid") - 2 * F.col("bb_std"))
    )

    # RSI (14-period)
    df = df.withColumn("price_diff", F.col("close") - F.lag("close", 1).over(sym_time))
    rsi_w = sym_time.rowsBetween(-13, 0)
    df = (
        df.withColumn("avg_gain",
            F.avg(F.when(F.col("price_diff") > 0, F.col("price_diff")).otherwise(0)).over(rsi_w))
          .withColumn("avg_loss",
            F.avg(F.when(F.col("price_diff") < 0, F.abs(F.col("price_diff"))).otherwise(0)).over(rsi_w))
          .withColumn("rs", F.col("avg_gain") / F.when(F.col("avg_loss") == 0, F.lit(1e-10)).otherwise(F.col("avg_loss")))
          .withColumn("rsi_14", 100 - (100 / (1 + F.col("rs"))))
    )

    # VWAP
    df = (
        df.withColumn("cum_tp_vol",
            F.sum(((F.col("high") + F.col("low") + F.col("close")) / 3) * F.col("volume")).over(sym_time))
          .withColumn("cum_vol", F.sum("volume").over(sym_time))
          .withColumn("vwap", F.col("cum_tp_vol") / F.col("cum_vol"))
    )

    return df.drop("price_diff", "avg_gain", "avg_loss", "rs", "cum_tp_vol", "cum_vol")


# ── Gold layer — daily summary ────────────────────────────────────────────────
def build_gold_daily(ticker_df: DataFrame) -> DataFrame:
    logger.info("🥇 Building gold daily summary ...")
    return (
        ticker_df.groupBy("symbol", "name", "date")
        .agg(
            F.first("open_price").alias("open"),
            F.max("high_price").alias("high"),
            F.min("low_price").alias("low"),
            F.last("last_price").alias("close"),
            F.sum("volume").alias("total_volume"),
            F.avg("vwap").alias("avg_vwap"),
            F.sum("trade_count").alias("total_trades"),
            F.avg("price_change_pct").alias("avg_price_change_pct"),
            F.count("*").alias("snapshot_count"),
        )
        .withColumn("pct_return",
            (F.col("close") - F.col("open")) / F.col("open") * 100)
        .withColumn("processed_at", F.current_timestamp())
    )


# ── Write helpers ─────────────────────────────────────────────────────────────
def write_silver(df: DataFrame, bucket: str, prefix: str, table: str) -> None:
    path = f"s3://{bucket}/{prefix}/{table}/"
    logger.info("🥈 Writing silver → %s", path)
    df.repartition("symbol").write.mode("overwrite").partitionBy("symbol").parquet(path)


def write_gold(df: DataFrame, bucket: str, prefix: str, table: str) -> None:
    path = f"s3://{bucket}/{prefix}/{table}/"
    logger.info("🥇 Writing gold → %s", path)
    df.repartition(1).write.mode("overwrite").partitionBy("symbol").parquet(path)


# ── Main ETL ──────────────────────────────────────────────────────────────────
def run_etl(params: dict = None) -> None:
    if params is None:
        params = get_job_params()

    spark = build_spark(params["JOB_NAME"])

    if RUNNING_IN_GLUE:
        glue_ctx = GlueContext(spark.sparkContext)
        job = Job(glue_ctx)
        job.init(params["JOB_NAME"], params)

    # 1. Read Bronze
    raw_df = read_bronze(
        spark,
        params["S3_BUCKET"],
        params["BRONZE_PREFIX"],
        params["PARTITION_DATE"],
    )

    # 2. Silver — Tickers
    ticker_df = extract_ticker(raw_df)
    write_silver(ticker_df, params["S3_BUCKET"], params["SILVER_PREFIX"], "tickers")

    # 3. Silver — Klines
    for interval in ("klines_1m", "klines_1h"):
        kline_df = extract_klines(raw_df, interval)
        kline_df = add_technical_indicators(kline_df)
        write_silver(kline_df, params["S3_BUCKET"], params["SILVER_PREFIX"],
                     f"klines_{interval.split('_')[1]}")

    # 4. Gold — Daily summary
    gold_df = build_gold_daily(ticker_df)
    write_gold(gold_df, params["S3_BUCKET"], params["GOLD_PREFIX"], "daily_summary")

    logger.info("🎉 ETL pipeline completed successfully!")

    if RUNNING_IN_GLUE:
        job.commit()

    spark.stop()


if __name__ == "__main__":
    run_etl()