import json
import os
import time
import boto3
import pytz
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Configuration from environment variables
INSTANCE_ID = os.getenv("INSTANCE_ID")
REGION = os.getenv("REGION")
REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])
S3_BUCKET = os.getenv("S3_STAGING_BUCKET")          
S3_PREFIX = os.getenv("S3_PREFIX", "connect-staging")  

EASTERN_TZ = pytz.timezone("America/New_York")
CONNECT_CLIENT = boto3.client("connect", region_name=REGION)
S3_CLIENT = boto3.client("s3", region_name=REGION)

def get_previous_interval_bounds(now_ny: datetime):
    """Calculate the previous 2-hour window in NY time."""
    current = now_ny.replace(minute=0, second=0, microsecond=0)
    hour = current.hour

    if hour == 0:
        start_local = EASTERN_TZ.localize(datetime.combine(now_ny.date() - timedelta(days=1), datetime.time(22, 0)))
        end_local = EASTERN_TZ.localize(datetime.combine(now_ny.date(), datetime.time(0, 0)))
        label = "22-00"
    else:
        start_hour = hour - 2
        start_local = EASTERN_TZ.localize(datetime.combine(now_ny.date(), datetime.time(start_hour, 0)))
        end_local = EASTERN_TZ.localize(datetime.combine(now_ny.date(), datetime.time(hour, 0)))
        label = f"{start_hour:02d}-{hour:02d}"

    end_local += timedelta(seconds=1)
    return start_local.astimezone(pytz.utc), end_local.astimezone(pytz.utc), label


def parse_datetime(ts) -> str | None:
    if not ts:
        return None
    try:
        if isinstance(ts, datetime):
            dt = ts.replace(tzinfo=pytz.utc) if ts.tzinfo is None else ts
        else:
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        return dt.astimezone(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Datetime parse error: {e} for {ts}")
        return None


def get_contact_details(contact_id: str):
    """Enrich with describe_contact."""
    try:
        resp = CONNECT_CLIENT.describe_contact(InstanceId=INSTANCE_ID, ContactId=contact_id)
        contact = resp.get('Contact', {})
        return (
            contact.get('CustomerEndpoint', {}).get('Address'),
            contact.get('TotalPauseCount', 0),
            contact.get('TotalPauseDurationInSeconds', 0),
            parse_datetime(contact.get('LastUpdateTimestamp')),
            parse_datetime(contact.get('QueueInfo', {}).get('EnqueueTimestamp')),
            contact.get('QueueTimeAdjustmentSeconds', 0),
            parse_datetime(contact.get('ConnectedToSystemTimestamp')),
        )
    except Exception as e:
        print(f"describe_contact failed for {contact_id}: {e}")
        return None, 0, 0, None, None, 0, None


def fetch_completed_calls(start_utc: datetime, end_utc: datetime):
    """Fetch calls using search_contacts + describe_contact enrichment."""
    rows = []
    next_token = None
    total = 0

    while True:
        params = {
            "InstanceId": INSTANCE_ID,
            "MaxResults": 100,
            "TimeRange": {"Type": "INITIATION_TIMESTAMP", "StartTime": start_utc, "EndTime": end_utc}
        }
        if next_token:
            params["NextToken"] = next_token

        try:
            response = CONNECT_CLIENT.search_contacts(**params)
        except ClientError as e:
            if e.response['Error']['Code'] == 'TooManyRequestsException':
                print("Rate limit hit, sleeping 2s...")
                time.sleep(2)
                continue
            print(f"search_contacts error: {e}")
            break

        batch = response.get("Contacts", [])
        total += len(batch)
        print(f"Retrieved {len(batch)} contacts (total: {total})")

        for contact in batch:
            contact_id = contact.get("Id")
            if not contact_id:
                continue

            raw_disconn = contact.get("DisconnectTimestamp")
            if not raw_disconn:
                continue

            raw_agent_conn = contact.get('AgentInfo', {}).get('ConnectedToAgentTimestamp')
            contact_duration = (raw_disconn - raw_agent_conn).total_seconds() if raw_disconn and raw_agent_conn else None

            customer_phone, agent_holds, customer_hold_duration, last_update, in_queue_time, queue_duration, conn_to_sys = get_contact_details(contact_id)

            row = {
                'init_contact_id': contact.get('InitialContactId'),
                'prev_contact_id': contact.get('PreviousContactId'),
                'contact_id': contact_id,
                'next_contact_id': None,
                'channel': contact.get('Channel'),
                'init_method': contact.get('InitiationMethod'),
                'init_time': parse_datetime(contact.get('InitiationTimestamp')),
                'disconn_time': parse_datetime(raw_disconn),
                'disconn_reason': None,
                'last_update_time': last_update,
                'agent_conn': parse_datetime(raw_agent_conn),
                'agent_id': contact.get('AgentInfo', {}).get('Id'),
                'agent_username': None,
                'agent_conn_att': None,
                'agent_afw_start': None,
                'agent_afw_end': None,
                'agent_afw_duration': None,
                'agent_interact_duration': None,
                'agent_holds': agent_holds,
                'agent_longest_hold': None,
                'queue_id': contact.get('QueueInfo', {}).get('Id'),
                'queue_name': None,
                'in_queue_time': in_queue_time,
                'out_queue_time': None,
                'queue_duration': queue_duration,
                'customer_voice': None,
                'customer_hold_duration': customer_hold_duration,
                'contact_duration': contact_duration,
                'sys_phone': None,
                'conn_to_sys': conn_to_sys,
                'customer_phone': customer_phone
            }
            rows.append(row)

            time.sleep(0.05)  # Rate limiting

        next_token = response.get("NextToken")
        if not next_token:
            break

    print(f"Total contacts fetched: {total}")
    return rows


def upload_to_s3(rows, interval_label: str):
    """Upload rows as JSON Lines to S3."""
    if not rows:
        print("No rows to upload.")
        return None

    timestamp = datetime.now(EASTERN_TZ).strftime("%Y%m%d_%H%M%S")
    s3_key = f"{S3_PREFIX}/{datetime.now().strftime('%Y/%m/%d')}/connect_calls_{interval_label}_{timestamp}.jsonl"

    # Convert rows to JSON Lines
    jsonl_data = "\n".join(json.dumps(row) for row in rows)

    try:
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=jsonl_data.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"Uploaded {len(rows)} records to s3://{S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise


def copy_from_s3_to_redshift(s3_key: str):
    """Execute COPY command from S3 into staging table."""
    if not s3_key:
        return

    copy_sql = f"""
        COPY connect.f_calls_staging
        FROM 's3://{S3_BUCKET}/{s3_key}'
        IAM_ROLE 'arn:aws:iam::___________'   
        FORMAT AS JSON 'auto'
        TIMEFORMAT 'auto';
    """

    try:
        with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(copy_sql)
                print(f"COPY command executed successfully for {s3_key}")

                cur.execute('CALL connect.insert_new_f_calls();')
                print("Stored procedure 'insert_new_f_calls()' executed.")
    except Exception as e:
        print(f"Redshift COPY or procedure error: {e}")
        raise


def lambda_handler(event, context):
    start_time = time.time()
    now_ny = datetime.now(EASTERN_TZ)

    start_utc, end_utc, interval_label = get_previous_interval_bounds(now_ny)
    print(f"Processing interval: {interval_label} (UTC: {start_utc} → {end_utc})")

    calls = fetch_completed_calls(start_utc, end_utc)

    if calls:
        s3_key = upload_to_s3(calls, interval_label)
        copy_from_s3_to_redshift(s3_key)
    else:
        print("No calls found in this interval.")

    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"Execution completed in {minutes}m {seconds}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": len(calls),
            "interval": interval_label,
            "s3_key": s3_key if 's3_key' in locals() else None
        })
    }
