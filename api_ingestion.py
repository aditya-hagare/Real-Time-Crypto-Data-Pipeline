"""
api_ingestion.py
────────────────────────────────────────────────
Real-time market data ingestion for:
  • Bitcoin   (BTCUSDT)  ← Binance API
  • Ethereum  (ETHUSDT)  ← Binance API
  • XRP       (XRPUSDT)  ← Binance API

All 3 from Binance — no API key needed.

LOCAL_MODE = True  →  saves JSON files locally  (for testing on VS Code)
LOCAL_MODE = False →  saves locally + uploads to AWS S3  (for production)

Author  : Data Engineering Project
Layer   : Ingestion (Bronze / Raw)
"""

import os
import json
import time
import logging
import requests
import boto3
from datetime import datetime, timezone
from botocore.exceptions import BotoCoreError, ClientError
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# Set True for local testing (no AWS needed)
# Set False when you have AWS configured
LOCAL_MODE = False

# Binance public API — no key needed
BINANCE_BASE_URL = "https://api.binance.com/api/v3"

# The 3 assets we track
ASSETS = [
    {"name": "Bitcoin",  "symbol": "BTCUSDT"},
    {"name": "Ethereum", "symbol": "ETHUSDT"},
    {"name": "XRP",      "symbol": "XRPUSDT"},
]

# AWS settings (only used when LOCAL_MODE = False)
S3_BUCKET      = os.getenv("S3_BUCKET", "crypto-data-lake-aditya")
S3_PREFIX      = "raw/market"
LOCAL_DATA_DIR = "data"
AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")

# Request settings
REQUEST_TIMEOUT = 10
MAX_RETRIES     = 3
RETRY_DELAY     = 2


# ══════════════════════════════════════════════════════════════════════════════
# BINANCE API HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _binance_get(endpoint: str, params: dict) -> Optional[dict | list]:
    """Generic Binance GET with retry logic."""
    url = f"{BINANCE_BASE_URL}/{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    logger.error("All retries exhausted for: %s", endpoint)
    return None


def fetch_ticker(symbol: str) -> Optional[dict]:
    """Live price + 24hr stats."""
    data = _binance_get("ticker/24hr", {"symbol": symbol})
    if data:
        logger.info("✅ %-10s  $%s", symbol, data.get("lastPrice"))
    return data


def fetch_orderbook(symbol: str) -> Optional[dict]:
    """Top 10 buy and sell orders."""
    return _binance_get("depth", {"symbol": symbol, "limit": 10})


def fetch_recent_trades(symbol: str) -> Optional[list]:
    """Last 50 trades."""
    return _binance_get("trades", {"symbol": symbol, "limit": 50})


def fetch_klines(symbol: str, interval: str, limit: int) -> Optional[list]:
    """OHLCV candle data."""
    raw = _binance_get("klines", {"symbol": symbol, "interval": interval, "limit": limit})
    if raw is None:
        return None
    keys = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore",
    ]
    return [dict(zip(keys, candle)) for candle in raw]


# ══════════════════════════════════════════════════════════════════════════════
# PAYLOAD BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_payload(asset: dict) -> dict:
    """Fetch all data for one asset and wrap it in a JSON envelope."""
    symbol = asset["symbol"]
    name   = asset["name"]
    logger.info("📡 Collecting %s (%s) …", name, symbol)

    return {
        "metadata": {
            "name":        name,
            "symbol":      symbol,
            "source":      "binance",
            "asset_class": "crypto",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_ver": "3.0.0",
        },
        "ticker":        fetch_ticker(symbol),
        "order_book":    fetch_orderbook(symbol),
        "recent_trades": fetch_recent_trades(symbol),
        "klines_1m":     fetch_klines(symbol, "1m", 60),
        "klines_5m":     fetch_klines(symbol, "5m", 12),
        "klines_1h":     fetch_klines(symbol, "1h", 24),
    }


# ══════════════════════════════════════════════════════════════════════════════
# LOCAL SAVE
# ══════════════════════════════════════════════════════════════════════════════

def save_locally(payload: dict, name: str) -> str:
    """Save JSON file to the /data folder."""
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    ts    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fpath = os.path.join(LOCAL_DATA_DIR, f"{name}_{ts}.json")
    with open(fpath, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("💾 Saved: %s", fpath)
    return fpath


# ══════════════════════════════════════════════════════════════════════════════
# S3 UPLOAD
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_s3(local_path: str, name: str) -> bool:
    """Upload to S3. Skipped automatically when LOCAL_MODE = True."""
    if LOCAL_MODE:
        logger.info("⏭️  LOCAL_MODE — skipping S3 for %s", name)
        return True

    now = datetime.now(timezone.utc)
    key = (
        f"{S3_PREFIX}/{name}/"
        f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/"
        f"{name}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.upload_file(local_path, S3_BUCKET, key,
                       ExtraArgs={"ContentType": "application/json"})
        logger.info("☁️  Uploaded → s3://%s/%s", S3_BUCKET, key)
        return True
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed for %s: %s", name, exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def ingest_asset(asset: dict) -> bool:
    """Full cycle: fetch → save locally → upload to S3."""
    payload    = build_payload(asset)
    local_path = save_locally(payload, asset["name"])
    return upload_to_s3(local_path, asset["name"])


def run_ingestion(assets: list[dict] = ASSETS) -> dict:
    """Run one ingestion cycle for all assets."""
    logger.info("🚀 Ingestion started — %s", [a["name"] for a in assets])
    results = {}
    for asset in assets:
        try:
            results[asset["name"]] = ingest_asset(asset)
        except Exception as exc:                  # noqa: BLE001
            logger.exception("Error ingesting %s: %s", asset["name"], exc)
            results[asset["name"]] = False
    passed = sum(results.values())
    logger.info("✅ Done — %d/%d succeeded: %s", passed, len(assets), results)
    return results


def run_realtime(
    assets: list[dict] = ASSETS,
    interval_seconds: float = 5.0,
    max_iterations: int | None = None,
) -> None:
    """Fetch all assets every 5 seconds. Press Ctrl+C to stop."""
    mode = "LOCAL (no S3)" if LOCAL_MODE else "CLOUD (with S3)"
    logger.info(
        "⚡ Real-time started | interval=%.1fs | mode=%s | assets=%s",
        interval_seconds, mode, [a["name"] for a in assets],
    )
    iteration = total_ok = total_fail = 0

    try:
        while True:
            iteration  += 1
            cycle_start = time.time()

            logger.info("━━━ Cycle #%d ━━━━━━━━━━━━━━━━━━━━━━━━━━", iteration)
            results     = run_ingestion(assets)
            total_ok   += sum(results.values())
            total_fail += sum(1 for v in results.values() if not v)
            logger.info(
                "📊 Totals — ✅ %d  ❌ %d  (%.1f%% success)",
                total_ok, total_fail,
                100 * total_ok / (total_ok + total_fail),
            )

            if max_iterations and iteration >= max_iterations:
                logger.info("✅ Reached max_iterations=%d — stopping.", max_iterations)
                break

            elapsed   = time.time() - cycle_start
            sleep_for = max(0.0, interval_seconds - elapsed)
            if elapsed > interval_seconds:
                logger.warning("⚠️  Cycle took %.1fs > %.1fs interval", elapsed, interval_seconds)
            else:
                logger.info("⏱  Done in %.2fs — sleeping %.2fs …", elapsed, sleep_for)
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user after %d cycles.", iteration)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Crypto real-time ingestion")
    parser.add_argument("--interval",       type=float, default=5.0,
                        help="Seconds between cycles (default: 5)")
    parser.add_argument("--max-iterations", type=int,   default=None,
                        help="Stop after N cycles (default: forever)")
    parser.add_argument("--once",           action="store_true",
                        help="Run one cycle and exit")
    parser.add_argument("--assets",         nargs="+",
                        choices=["Bitcoin", "Ethereum", "XRP"],
                        default=None,
                        help="Run specific assets only")
    args = parser.parse_args()

    selected = ASSETS
    if args.assets:
        selected = [a for a in ASSETS if a["name"] in args.assets]

    if args.once:
        run_ingestion(selected)
    else:
        run_realtime(
            assets=selected,
            interval_seconds=args.interval,
            max_iterations=args.max_iterations,
        )