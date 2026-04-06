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

EASTERN_TZ = pytz.timezone("America/New_York")
CONNECT_CLIENT = boto3.client("connect", region_name=REGION)


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
            "TimeRange": {
                "Type": "INITIATION_TIMESTAMP",
                "StartTime": start_utc,
                "EndTime": end_utc
            }
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

            rows.append((
                contact.get('InitialContactId'),
                contact.get('PreviousContactId'),
                contact_id,
                None,  # next_contact_id
                contact.get('Channel'),
                contact.get('InitiationMethod'),
                parse_datetime(contact.get('InitiationTimestamp')),
                parse_datetime(raw_disconn),
                None,  # disconn_reason (not always available)
                last_update,
                parse_datetime(raw_agent_conn),
                contact.get('AgentInfo', {}).get('Id'),
                None,  # agent_username
                None,  # agent_conn_att
                None,  # afw fields
                None,
                None,
                None,
                agent_holds,
                None,  # longest_hold
                contact.get('QueueInfo', {}).get('Id'),
                None,  # queue_name
                in_queue_time,
                None,  # out_queue_time
                queue_duration,
                None,  # customer_voice
                customer_hold_duration,
                contact_duration,
                None,  # sys_phone
                conn_to_sys,
                customer_phone
            ))

            time.sleep(0.05)  # Polite rate limiting

        next_token = response.get("NextToken")
        if not next_token:
            break

    print(f"Total contacts fetched: {total}")
    return rows


def insert_into_redshift(rows):
    if not rows:
        print("No rows to insert.")
        return

    insert_sql = """
        INSERT INTO connect.f_calls_staging (
            init_contact_id, prev_contact_id, contact_id, next_contact_id,
            channel, init_method, init_time, disconn_time, disconn_reason,
            last_update_time, agent_conn, agent_id, agent_username,
            agent_conn_att, agent_afw_start, agent_afw_end, agent_afw_duration,
            agent_interact_duration, agent_holds, agent_longest_hold,
            queue_id, queue_name, in_queue_time, out_queue_time, queue_duration,
            customer_voice, customer_hold_duration, contact_duration,
            sys_phone, conn_to_sys, customer_phone
        ) VALUES %s
    """

    try:
        with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=1000)
                print(f"Inserted {len(rows)} rows into staging table.")

                cur.execute('CALL connect.insert_new_f_calls();')
                print("Stored procedure executed successfully.")
    except Exception as e:
        print(f"Redshift insert error: {e}")


def lambda_handler(event, context):
    start_time = time.time()
    now_ny = datetime.now(EASTERN_TZ)

    start_utc, end_utc, interval = get_previous_interval_bounds(now_ny)
    print(f"Processing interval: {interval} (UTC: {start_utc} → {end_utc})")

    calls = fetch_completed_calls(start_utc, end_utc)
    insert_into_redshift(calls)

    duration = time.time() - start_time
    print(f"Execution completed in {int(duration // 60)}m {int(duration % 60)}s")

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": len(calls), "interval": interval})
    }
