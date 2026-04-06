# Connect API Collector Lambda (Backup Pipeline)

This Lambda function serves as a **resilient backup ingestion path** for Amazon Connect call data when the primary Kinesis Firehose CTR pipeline is unavailable or unstable.

It was originally built during a production incident caused by a Salesforce integration that disrupted the standard CTR delivery.

---

## Purpose

- Fetches completed calls directly from Amazon Connect using the `search_contacts` and `describe_contact` APIs
- Processes data for the **previous 2-hour window** (New York time)
- Writes records as JSON Lines to an **S3 staging bucket**
- Triggers a `COPY` command from S3 into Redshift staging table
- Executes a stored procedure to merge new records into the main fact table (`connect.f_calls`)

---

## Architecture Flow

**EventBridge (every 2 hours)** → **This Lambda** → **S3 Staging Bucket** → **Redshift** (`f_calls_staging` → `f_calls`)

**Key Advantages Over Direct INSERT:**
- Uses Redshift's optimized `COPY` command (much faster and more scalable)
- Consistent with the primary Firehose pipeline architecture
- Better performance and lower load on Redshift slices

---

## Key Features

- **2-hour rolling window** calculation based on America/New_York timezone
- Enriches call data using `describe_contact` API
- Polite rate limiting to respect AWS Connect API limits
- Uploads data as JSON Lines (.jsonl) to S3 with organized partitioning (`year/month/day/`)
- Executes `COPY` from S3 into staging table
- Runs stored procedure for SCD Type 1 merge (deduplication)
- Comprehensive logging for monitoring

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

---

## How It Works

1. Triggered by EventBridge every 2 hours
2. Calculates the previous 2-hour time window
3. Calls `search_contacts()` to retrieve completed calls
4. Enriches each call with `describe_contact()`
5. Uploads records to S3 as JSON Lines
6. Executes `COPY` command from S3 into `connect.f_calls_staging`
7. Calls `connect.insert_new_f_calls()` stored procedure to merge data into the main table

---

## Tech Stack

- Python 3.9
- boto3 (Amazon Connect + S3)
- psycopg2 (for executing COPY and stored procedure)
- pytz (timezone handling)

---

## Environment Variables

| Variable                | Description                                      | Example |
|-------------------------|--------------------------------------------------|---------|
| `INSTANCE_ID`           | Amazon Connect Instance ID                       | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `REGION`                | AWS Region                                       | `us-east-1` |
| `REDSHIFT_CONFIG`       | JSON string with Redshift connection details     | `{...}` |
| `S3_STAGING_BUCKET`     | S3 bucket for staging files                      | `my-company-connect-staging` |
| `S3_PREFIX`             | Optional prefix inside the bucket                | `connect-staging` |

---

## Redshift Load Pattern

```sql
COPY connect.f_calls_staging
FROM 's3://_____________.'
IAM_ROLE 'arn:aws:_________________'
FORMAT AS JSON 'auto'
```

## Lessons Learned

- Direct API collection is a reliable fallback when vendor integrations (like Salesforce) break standard CTR delivery.
- Using S3 + COPY is the recommended pattern for loading data into Redshift at scale.
- Scheduling incremental windows (2 hours) provides near real-time data with good safety margins.

This Lambda ensures data continuity for contact center analytics even during upstream disruptions.
TIMEFORMAT 'auto';
