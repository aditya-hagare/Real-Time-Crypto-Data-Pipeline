# Real-Time-Crypto-Data-Pipeline
# 🚀 End-to-End Data Engineering Pipeline — AWS + Crypto
---

## 📌 Project Overview

A **production-grade, cloud-native data pipeline** that ingests real-time cryptocurrency market data from the Binance API, processes it through a multi-layer data lake architecture on AWS, stores analytics in Amazon Redshift, and visualizes insights via a Power BI dashboard — all automated with Windows Task Scheduler.

---

## 🏗 Architecture

```
Binance API (Real-time)
        ↓
Python Ingestion Script (every 30 seconds)
        ↓
AWS S3 Data Lake
  🥉 Bronze → raw/market/          (raw JSON)
  🥈 Silver → processed/market/    (clean Parquet)
  🥇 Gold   → analytics/market/    (aggregated Parquet)
        ↓
AWS Glue + PySpark (ETL Processing)
        ↓
Amazon Redshift Serverless (Data Warehouse)
        ↓
Power BI Dashboard (Analytics & Visualization)
        ↓
Task Scheduler (Automation — every 15 minutes)
```

---

## 🎯 What This Project Does

1. **Collects** real-time crypto prices for Bitcoin, Ethereum and XRP from Binance API every 30 seconds
2. **Stores** raw JSON data in AWS S3 Bronze layer with Hive-style partitioning
3. **Processes** data using AWS Glue PySpark ETL — cleaning, validating, computing technical indicators
4. **Stores** clean analytics data in Amazon Redshift Serverless data warehouse
5. **Visualizes** insights in Power BI dashboard with live price cards, trend charts and volume analysis
6. **Automates** the entire pipeline using Windows Task Scheduler

---

## 🛠 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | Python + Requests | Pull live data from Binance API |
| **Storage** | AWS S3 | Data lake (Bronze/Silver/Gold layers) |
| **Processing** | AWS Glue + PySpark | ETL, cleaning, technical indicators |
| **Warehouse** | Amazon Redshift Serverless | Analytics SQL queries |
| **Dashboard** | Power BI Desktop | Charts and KPI cards |
| **Automation** | Windows Task Scheduler | Pipeline orchestration |
| **SDK** | boto3 | Python AWS integration |

---

## 📊 Data Pipeline Layers (Medallion Architecture)

```
🥉 Bronze Layer  →  s3://bucket/raw/market/{coin}/YYYY/MM/DD/HH/
   Raw JSON files exactly as received from Binance API

🥈 Silver Layer  →  s3://bucket/processed/market/{table}/
   Clean Parquet files with:
   • Proper data types (string → float, epoch → timestamp)
   • Removed nulls and duplicates
   • Technical indicators (SMA, RSI, Bollinger Bands, VWAP)

🥇 Gold Layer    →  s3://bucket/analytics/market/daily_summary/
   Aggregated daily stats per coin:
   • Open, High, Low, Close prices
   • Total volume and trades
   • Daily return percentage
   • Average VWAP
```

---

## 📈 Technical Indicators Computed

| Indicator | Description |
|-----------|-------------|
| **SMA 7/14/21** | Simple Moving Averages |
| **Bollinger Bands** | ±2 std deviation around 20-period SMA |
| **RSI (14)** | Relative Strength Index |
| **VWAP** | Volume Weighted Average Price |

---

## 📁 Project Structure

```
aws-data-engineering-project/
│
├── api_ingestion.py      # Binance API → S3 Bronze layer
├── spark_job.py          # AWS Glue PySpark ETL job
├── pipeline_dag.py       # Apache Airflow DAG
├── crypto_data.json      # Sample Binance API payload
└── README.md             # Project documentation
```

---

## ⚡ Quick Start

### Prerequisites
- Python 3.12+
- AWS account with S3, Glue, Redshift configured
- Power BI Desktop installed

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/aws-data-engineering-project.git
cd aws-data-engineering-project
```

### 2. Install Dependencies
```bash
pip install requests boto3 awscli pandas pyarrow
```

### 3. Configure AWS
```bash
aws configure
# Enter your AWS Access Key, Secret Key, Region (ap-south-1)
```

### 4. Run Ingestion
```bash
# Run every 30 seconds forever
python api_ingestion.py --interval 30

# Run once
python api_ingestion.py --once

# Run specific coins only
python api_ingestion.py --assets Bitcoin Ethereum
```

### 5. Deploy Glue Job
```bash
# Upload to S3
aws s3 cp spark_job.py s3://your-bucket/scripts/

# Create job in AWS Glue Console
# Script: s3://your-bucket/scripts/spark_job.py
# Glue version: 4.0
# Worker type: G.1X
# Workers: 2
```

### 6. Connect Power BI
```
Power BI Desktop
→ Get Data → Amazon Redshift
→ Server: your-redshift-endpoint
→ Database: crypto_db
→ Table: market.crypto_daily
```

---

## 🔄 Automation Schedule

| Task | Frequency | Tool |
|------|-----------|------|
| Data Ingestion | Every 30 seconds | Task Scheduler |
| Glue ETL Job | Every 15 minutes | Task Scheduler |
| Redshift Refresh | Every 30 minutes | Task Scheduler |
| Power BI Refresh | Manual / 30 min | Power BI Desktop |

---

## 📊 Power BI Dashboard

The dashboard includes:
- **4 KPI Cards** — Live BTC, ETH, XRP prices + Total Volume
- **Price Trend Chart** — Line chart showing price history
- **Daily Return %** — Bar chart showing performance
- **Trading Volume** — Column chart by coin
- **High vs Low Range** — Price range comparison
- **Interactive Slicer** — Filter all charts by coin

---

## 💡 Skills Demonstrated

✅ Real-time API data ingestion with retry logic\
✅ Cloud data lake design (Medallion Architecture)\
✅ PySpark ETL with technical indicator computation\
✅ AWS Glue managed ETL service\
✅ Amazon Redshift Serverless data warehouse\
✅ Power BI dashboard development\
✅ Pipeline automation\
✅ AWS S3 partitioning strategy\
✅ Data quality validation\
✅ Production patterns: retries, idempotency, logging

---

## 🔒 Security Notes

- AWS credentials stored in `~/.aws/credentials` — never in code
- S3 bucket has IAM role-based access control
- Redshift accessed via VPC security group on port 5439
- All data encrypted at rest in S3 (SSE-S3)



---

## ⭐ If you found this helpful, please give it a star!
