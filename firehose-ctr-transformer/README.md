# Firehose CTR Transformer Lambda

This Lambda function transforms raw **Amazon Connect Contact Trace Records (CTRs)** delivered by **Kinesis Data Firehose** into a flat, analytics-friendly structure before loading into Amazon Redshift.

---

## Purpose

- Decodes base64-encoded CTR records from Firehose
- Flattens deeply nested JSON (Agent, Queue, CustomerEndpoint, etc.)
- Performs **idempotency checks** using DynamoDB to prevent duplicate processing
- Normalizes all timestamps to `America/New_York` timezone
- Returns transformed records back to Firehose for delivery to Redshift

---

## Key Features

- **Idempotent processing** via DynamoDB conditional writes (`ProcessedCTR` table)
- Robust error handling – gracefully drops malformed, empty, or duplicate records
- Consistent datetime formatting for reporting
- Lightweight and fast (designed to run within Firehose transformation constraints)

---

## Redshift Loading

After transformation, Firehose writes the records as JSON to an S3 bucket. Redshift then loads them using a `COPY` command similar to this:

```sql
COPY connect.f_calls_staging
FROM 's3://______'
IAM_ROLE '____'
MANIFEST
FORMAT AS JSON 'auto';
```
---

## Data Format

Data is currently written to S3 as **JSON Lines (.jsonl)**.

### Why JSON Lines was chosen:
- Simple and quick to implement during the urgent outage recovery
- Excellent compatibility with Redshift's `FORMAT AS JSON 'auto'`
- Easy debugging and human-readable during development

### Trade-offs acknowledged:
- JSON is more verbose and slower to load compared to columnar formats
- Higher storage and processing cost at scale

**Planned Enhancement**:  
Convert output to **Parquet** format in a future iteration to improve COPY performance, reduce storage costs, and take full advantage of Redshift's columnar capabilities.

## How It Works

1. Firehose invokes this Lambda with a batch of records
2. For each record:
   - Base64 decode → JSON parse
   - Extract `ContactId` and check for duplicates
   - Flatten nested fields
   - Convert timestamps to Eastern Time
   - Base64 encode the transformed record
3. Returns `Ok` or `Dropped` status to Firehose

---

## Tech Stack

- Python 3.9
- boto3 (DynamoDB)
- pytz (timezone handling)

---

## Configuration Requirements

- **DynamoDB Table**: `ProcessedCTR` (with `ContactId` as partition key)
- **IAM Permissions**:
  - `dynamodb:PutItem` on the `ProcessedCTR` table
  - CloudWatch Logs write permissions

---

## Deployment

This Lambda is configured as a **Data Transformation** function on a Kinesis Data Firehose delivery stream with:
- Destination: Amazon Redshift Serverless
- Buffer size: 2 MiB
- Buffer interval: 900 seconds

---

## Related Components

- Main pipeline uses this transformer before loading into `connect.f_calls`
- See sibling folder `connect-api-collector/` for the backup ingestion Lambda (used during outages)
