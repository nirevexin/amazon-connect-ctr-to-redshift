# Amazon Connect CTR ETL Pipeline to Redshift

Robust, serverless ETL solution for ingesting **Amazon Connect Contact Trace Records (CTRs)** into Amazon Redshift for contact center analytics and reporting.

## Structure

```
amazon-connect-ctr-to-redshift/
├── 01_firehose-ctr-transformer/
│   └── lambda_function.py
├── 02_connect-api-collector/
│   └── lambda_function.py
├── .gitignore
└── README.md
```

## Overview

This project implements two complementary ingestion paths:

- **Primary Pipeline**: Real-time ingestion via **Kinesis Data Firehose** + **Lambda transformation** (flattening + deduplication).
- **Backup Collector**: Resilient fallback using direct **AWS Connect APIs** (`search_contacts` + `describe_contact`) running every 2 hours.

The backup solution was built during a production incident when a Salesforce integration disrupted the standard CTR delivery.

## Architecture

- **Primary**: Direct PUT → Kinesis Firehose → Lambda (Python) → S3 → Redshift COPY (JSON)
- **Fallback**: EventBridge (every 2h) → Lambda → Connect APIs → S3 → Redshift COPY (JSON) 

**Key Features**
- Idempotent processing using DynamoDB conditional writes
- Timezone normalization to `America/New_York`
- Graceful handling of malformed/empty records
- Staging table + merge pattern for Redshift
- Rate-limit aware API calls

## Repository Structure

- `01_firehose-ctr-transformer/` – Lambda for Firehose data transformation
- `02_connect-api-collector/` – Backup collector Lambda

## Tech Stack

- **AWS**: Kinesis Data Firehose, Lambda (Python 3.9+), DynamoDB, Redshift Serverless, EventBridge, Amazon Connect, S3
- **Libraries**: boto3, psycopg2, pytz

## Lessons Learned

- Importance of monitoring CTR delivery health and having fallback mechanisms.
- Direct API collection is effective for bridging gaps during upstream changes.
- Staging + stored procedure pattern works well for controlled Redshift loads.

**Potential Improvements**
- Migrate from JSON Lines to Parquet format for S3 files to improve COPY performance, reduce storage costs, and leverage Redshift’s columnar capabilities
- Containerize or move heavier logic to AWS Glue

## License

MIT
